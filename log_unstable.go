// Copyright 2024 BINARY Members
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

// unstable for storage unstable raft log entries
// unstable.entries[i] has raft log position i+unstable.offset
//
// EXAMPLE:
// 1. Assume that we have three log iter at first
//
//	u := &unstable{
//		 entries: []pb.Entry{
//		     {Index: 1, Term: 1},
//		     {Index: 2, Term: 1},
//		     {Index: 3, Term: 1},
//		 },
//		 offset: 1,
//	     offsetInProgress: 1,
//	}
//
// 2. Call acceptInProgress
// This will update offsetInProgress to the next index of last item in entries
// offsetInProgress = 1 + 3 = 4
// Which means that log entries with index 1, 2, 3 are being written to stable storage
//
// 3. Call nextEntries
// log items which are not written to stable storage will be return
// Since offsetInProgress is 4, all entries are written to storage, nextEntries return nil
//
// 4. Append new entries
//
//	newEntries := []pb.Entry{
//		{Index: 4, Term: 1},
//		{Index: 5, Term: 1},
//	}
//	u.append(newEntries)
//
// entries now have entries with index 1, 2, 3, 4, 5
// offset = 1
// offsetInProgress = 4
//
// 5. Call nextEntries
// entries with index 4 and 5 will be return
type unstable struct {
	snapshot *rt.Snapshot
	entries  []rt.Entry
	// initialize to Storage.LastIndex + 1
	// entries[i] == raftlog[i+offset]
	// raftlog[i] == entries[i-offset]
	offset int64
	// entries[:offsetInProgress-offset] are being written to Storage
	// entries[offsetInProgress:] will be written to Storage (current is unstable)
	// NOTE: offset <= offsetInProgress
	offsetInProgress   int64
	snapshotInProgress bool
	logger             Logger
}

func (u *unstable) maybeFirstIndex() (int64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.LastIndex + 1, true
	}
	return 0, false
}

func (u *unstable) maybeLastIndex() (int64, bool) {
	if n := len(u.entries); n != 0 {
		return u.offset + int64(n) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.LastIndex, true
	}
	return 0, false
}

func (u *unstable) maybeTerm(idx int64) (int64, bool) {
	if idx < u.offset {
		if u.snapshot != nil {
			return u.snapshot.Metadata.LastTerm, true
		}
		return 0, false
	}

	li, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if idx > li {
		return 0, false
	}

	return u.entries[idx-u.offset].Term, true
}

func (u *unstable) nextEntries() []rt.Entry {
	inProgress := int(u.offsetInProgress - u.offset)
	if len(u.entries) == inProgress {
		return nil
	}
	return u.entries[inProgress:]
}

func (u *unstable) nextSnapshot() *rt.Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// stableTo the entry at idx (include log entry at idx)
func (u *unstable) stableTo(idx, term int64) {
	t, ok := u.maybeTerm(idx)
	if !ok {
		u.logger.Infof("log entry [idx:%v] is missing from unstable log", idx)
		return
	}
	if idx < u.offset {
		u.logger.Infof("log entry [idx:%v] matched unstable snapshot", idx)
		return
	}
	if term != t {
		u.logger.Infof("log entry [idx:%v] mismatched term [expected:%v, current%v]", idx, t, term)
		return
	}
	num := idx + 1 - u.offset
	u.entries = u.entries[num:]
	u.offset = idx + 1
	u.offsetInProgress = max(u.offsetInProgress, u.offset)
	u.shrinkEntriesArray()
}

func (u *unstable) stableSnapshotTo(idx int64) {
	if u.snapshot != nil && u.snapshot.Metadata.LastIndex == idx {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

func (u *unstable) restore(snap rt.Snapshot) {
	u.offset = snap.Metadata.LastIndex + 1
	u.offsetInProgress = u.offset
	u.entries = nil
	u.snapshot = &snap
	u.snapshotInProgress = false
}

// acceptInProgress marks all entries and the snapshot, if any, in the unstable
// as having begun the process of being written to storage. The entries/snapshot
// will no longer be returned from nextEntries/nextSnapshot. However, new
// entries/snapshots added after a call to acceptInProgress will be returned
// from those methods, until the next call to acceptInProgress.
func (u *unstable) acceptInProgress() {
	n := len(u.entries)
	if n > 0 {
		u.offsetInProgress = u.entries[n-1].Index + 1
	}
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

func (u *unstable) append(entries ...rt.Entry) {
	from := entries[0].Index
	switch {
	case from == u.offset+int64(len(u.entries)):
		// from is the next index in the u.entries, append directly
		u.entries = append(u.entries, entries...)
	case from <= u.offset:
		u.logger.Infof("replace the unstable entries from %v", from)
		u.entries = entries
		u.offset = from
		u.offsetInProgress = u.offset
	default:
		u.logger.Infof("truncate the unstable entries before %v", from)
		keep := u.slice(u.offset, from)
		u.entries = append(keep, entries...)
		u.offsetInProgress = min(u.offsetInProgress, from)
	}
}

func (u *unstable) slice(low, high int64) []rt.Entry {
	u.mustCheckOutOfBounds(low, high)
	// NOTE: use full slice expressions
	return u.entries[low-u.offset : high-u.offset : high-u.offset]
}

func (u *unstable) mustCheckOutOfBounds(low, high int64) {
	if low > high {
		u.logger.Panicf("invalid unstable slice low [%v] > high [%v]", low, high)
	}
	bound := u.offset + int64(len(u.entries))
	if low < u.offset || high > bound {
		u.logger.Panicf("slice out of bound [low:%v, high:%v, offset:%v, bound:%v]", low, high, u.offset, bound)
	}
}

func (u *unstable) shrinkEntriesArray() {
	const rate = 2
	if len(u.entries) == 0 {
		u.entries = nil
		return
	}
	if len(u.entries)*rate < cap(u.entries) {
		newEntries := make([]rt.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}
