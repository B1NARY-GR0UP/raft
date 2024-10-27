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
	"errors"
	"fmt"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

// log Raft log
type log struct {
	// Volatile state on all servers
	commitIndex int64
	lastApplied int64
	// INVARIANT: lastApplied <= lastApplying && lastApplying <= commitIndex
	lastApplying int64
	unstable     unstable
	storage      Storage
	logger       Logger
}

func newLog(storage Storage, logger Logger) *log {
	if storage == nil {
		panic("storage must not be nil")
	}
	// bootstrap firstIndex is 1
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	// bootstrap lastIndex is 0
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// NOTE: Raft log's index is start with 1
	//
	// commitIndex and lastApplied with be init with 0
	// to represent that no log committed or applied
	return &log{
		commitIndex:  firstIndex - 1,
		lastApplied:  firstIndex - 1,
		lastApplying: firstIndex - 1,
		unstable: unstable{
			offset:           lastIndex + 1,
			offsetInProgress: lastIndex + 1,
			logger:           logger,
		},
		storage: storage,
		logger:  logger,
	}
}

func (l *log) String() string {
	return fmt.Sprintf("commitIndex=%d, lastApplied=%d, lastApplying=%d, unstable.offset=%d, unstable.offsetInProgress=%d, len(unstable.Entries)=%d",
		l.commitIndex, l.lastApplied, l.lastApplying, l.unstable.offset, l.unstable.offsetInProgress, len(l.unstable.entries))
}

func (l *log) entriesFrom(low int64) ([]rt.Entry, error) {
	if low > l.lastIndex() {
		return nil, nil
	}
	return l.slice(low, l.lastIndex()+1)
}

func (l *log) entriesTo(high int64) ([]rt.Entry, error) {
	if high < l.firstIndex() {
		return nil, nil
	}
	return l.slice(l.firstIndex(), high)
}

func (l *log) slice(low, high int64) ([]rt.Entry, error) {
	if err := l.mustCheckOutOfBounds(low, high); err != nil {
		return nil, err
	}

	if low == high {
		return nil, nil
	}

	// read from unstable log
	if low >= l.unstable.offset {
		entries := l.unstable.slice(low, high)
		// NOTE: use full slice expressions
		return entries[:len(entries):len(entries)], nil
	}

	// read from stable log
	return l.storage.Entries(low, min(high, l.unstable.offset))
}

func (l *log) mustCheckOutOfBounds(low, high int64) error {
	if low > high {
		l.logger.Panicf("invalid slice low [%v] > high [%v]", low, high)
	}

	if low < l.firstIndex() {
		return ErrCompacted
	}
	if high > l.lastIndex()+1 {
		l.logger.Panicf("slice out of bound [low:%v, high:%v, firstIndex:%v, lastIndex:%v]", low, high, l.firstIndex(), l.lastIndex())
	}
	return nil
}

// isUpToDate determine if the caller's log is at least as up-to-date as callee's log
//
// caller's log is "more complete" than callee's log: condition 1 || condition 2,
// caller's log is "as latest as" callee's log: condition 3
//
// 5.4.1 Election restriction
// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date
func (l *log) isUpToDate(lastIndex, lastTerm int64) bool {
	// condition 1
	return lastTerm > l.lastTerm() ||
		// condition 2
		(lastTerm == l.lastTerm() && lastIndex > l.lastIndex()) ||
		// condition 3
		(lastTerm == l.lastTerm() && lastIndex == l.lastIndex())
}

// matchTerm
// check if the entry at idx has the same term
func (l *log) matchTerm(idx, term int64) bool {
	t, err := l.term(idx)
	if err != nil {
		return false
	}
	return t == term
}

func (l *log) term(idx int64) (int64, error) {
	if t, ok := l.unstable.maybeTerm(idx); ok {
		return t, nil
	}
	// NOTE:
	// The valid term range is [firstIndex-1, lastIndex].
	// Even though the entry at firstIndex-1 is compacted away,
	// its term is available for matching purposes when doing log appends.
	if idx < l.firstIndex()-1 {
		return 0, ErrCompacted
	}
	if idx > l.lastIndex() {
		return 0, ErrUnavailable
	}

	term, err := l.storage.Term(idx)
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (l *log) lastTerm() int64 {
	term, err := l.term(l.lastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

func (l *log) firstIndex() int64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	idx, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return idx
}

func (l *log) lastIndex() int64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	idx, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return idx
}

func (l *log) snapshot() (rt.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// commitTo
// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (l *log) commitTo(idx int64) {
	if idx <= l.commitIndex {
		return
	}
	l.commitIndex = min(idx, l.lastIndex())
}

func (l *log) stableTo(idx, term int64) {
	l.unstable.stableTo(idx, term)
}

func (l *log) stableSnapshotTo(idx int64) {
	l.unstable.stableSnapshotTo(idx)
}

func (l *log) applyTo(idx int64) {
	if l.commitIndex < idx || idx < l.lastApplied {
		l.logger.Panicf("apply index is out of range [commitIndex:%v, lastApplied:%v]", l.commitIndex, l.lastApplied)
	}
	l.lastApplied = idx
	l.lastApplying = max(l.lastApplying, idx)
}

func (l *log) append(prevLogIndex int64, entries ...rt.Entry) {
	ci := l.findConflict(entries...)
	switch {
	case ci == 0:
	case ci <= l.commitIndex:
		l.logger.Panicf("log entry [idx:%v] conflict with committed entry [commitIndex:%v]", ci, l.commitIndex)
	default:
		offset := prevLogIndex + 1
		if ci-offset > int64(len(entries)) {
			l.logger.Panicf("index [idx:%v] out of range [range:%v]", ci-offset, len(entries))
		}
		l.appendUnstable(entries[ci-offset:]...)
	}
}

func (l *log) appendUnstable(entries ...rt.Entry) {
	if len(entries) == 0 {
		return
	}
	if after := entries[0].Index - 1; after < l.commitIndex {
		l.logger.Panicf("after [after:%v] is out of range [committed:%v]", after, l.commitIndex)
	}
	l.unstable.append(entries...)
}

func (l *log) findConflict(entries ...rt.Entry) int64 {
	for _, entry := range entries {
		if !l.matchTerm(entry.Index, entry.Term) {
			if entry.Index <= l.lastIndex() {
				l.logger.Infof("found conflict log entry [idx:%v, existing term:%v, conflicting term:%v]",
					entry.Index, l.zeroTermOnOutOfBounds(l.term(entry.Index)), entry.Term)
			}
			return entry.Index
		}
	}
	return 0
}

func (l *log) restore(snap rt.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [lastIndex: %d, lastTerm: %d]", l, snap.Metadata.LastIndex, snap.Metadata.LastTerm)
	l.commitIndex = snap.Metadata.LastIndex
	l.unstable.restore(snap)
}

func (l *log) hasNextUnstableEntries() bool {
	return len(l.unstable.nextEntries()) > 0
}

func (l *log) hasNextUnstableSnapshot() bool {
	return l.unstable.nextSnapshot() != nil
}

func (l *log) hasNextOrInProgressSnapshot() bool {
	return l.unstable.snapshot != nil
}

func (l *log) hasNextOrInProgressUnstableEntries() bool {
	return len(l.unstable.entries) > 0
}

func (l *log) hasNextCommittedEntries() bool {
	// If we have a snapshot to apply, don't also return any committed
	// entries.
	// Doing so raises questions about what should be applied first.
	if l.hasNextOrInProgressSnapshot() {
		return false
	}
	return l.lastApplying < l.commitIndex
}

func (l *log) nextUnstableEntries() []rt.Entry {
	return l.unstable.nextEntries()
}

func (l *log) nextUnstableSnapshot() *rt.Snapshot {
	return l.unstable.nextSnapshot()
}

func (l *log) nextCommittedEntries() []rt.Entry {
	if ok := l.hasNextCommittedEntries(); !ok {
		return nil
	}
	low, high := l.lastApplying+1, l.commitIndex+1 // [low, high)
	entries, err := l.slice(low, high)
	if err != nil {
		l.logger.Panicf("error getting un-applied entries: %v", err)
	}
	return entries
}

func (l *log) acceptUnstable() {
	l.unstable.acceptInProgress()
}

func (l *log) acceptApplying(idx int64) {
	if l.commitIndex < idx {
		l.logger.Panicf("applying index [applying:%v] is out of range [commitIndex:%v]", idx, l.commitIndex)
	}
	l.lastApplying = idx
}

func (l *log) zeroTermOnOutOfBounds(term int64, err error) int64 {
	if err == nil {
		return term
	}
	if errors.Is(err, ErrCompacted) || errors.Is(err, ErrUnavailable) {
		return 0
	}
	l.logger.Panicf("unexpected error: %v", err)
	return 0
}
