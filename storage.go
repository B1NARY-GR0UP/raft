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
	"sync"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

var _ Storage = (*MemoryStorage)(nil)

// Storage log storage
//
// NOTE:
// All the idx here represent the raft log index
type Storage interface {
	InitialState() (rt.PersistentState, rt.ConfState, error)
	Entries(low, high int64) ([]rt.Entry, error)
	Term(idx int64) (int64, error)
	FirstIndex() (int64, error)
	LastIndex() (int64, error)
	// Snapshot returns the most recent snapshot
	Snapshot() (rt.Snapshot, error)
}

var (
	ErrCompacted         = errors.New("request index is unavailable due to compaction")
	ErrUnavailable       = errors.New("entries not available")
	ErrSnapshotOutOfDate = errors.New("request index is older than existing snapshot")
)

// MemoryStorage
//
// entries[0] is a dummy entry used to sync with raft log index because the first index of raft log is 1
// To make is easier to understand, consider the following example
//
// Without compacted entries:
// Index [0 1 2 3]
// Term  [0 1 2 2]
// offset = 0
//
// With compacted entries:
// Index [6 7 8 9]
// Term  [4 4 5 5]
// offset = 6
type MemoryStorage struct {
	mu              sync.RWMutex
	persistentState rt.PersistentState
	snapshot        rt.Snapshot
	// entries[i] has raft log position i+snapshot.Metadata.LastIndex
	entries []rt.Entry
}

// NewMemoryStorage initializes an empty MemoryStorage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// init dummy entry at entries[0]
		entries: make([]rt.Entry, 1),
	}
}

// offset represents the dummy entry's index (in raft log)
func (ms *MemoryStorage) offset() int64 {
	return ms.entries[0].Index
}

func (ms *MemoryStorage) InitialState() (rt.PersistentState, rt.ConfState, error) {
	var cs rt.ConfState
	if !IsEmptySnapshot(ms.snapshot) {
		cs = *ms.snapshot.Metadata.ConfState
	}
	return ms.persistentState, cs, nil
}

func (ms *MemoryStorage) SetPersistentState(ps rt.PersistentState) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.persistentState = ps
	return nil
}

func (ms *MemoryStorage) Entries(low, high int64) ([]rt.Entry, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	offset := ms.offset()
	if low <= offset {
		return nil, ErrCompacted
	}
	if high > ms.lastIndex()+1 {
		getLogger().Panicf("high index [%v] out of range [%v]", high, ms.lastIndex())
	}
	if len(ms.entries) == 1 {
		return nil, ErrUnavailable
	}
	entries := ms.entries[low-offset : high-offset]
	// NOTE: use full slice expressions to prevent caller's operation to the bottom array
	return entries[:len(entries):len(entries)], nil
}

func (ms *MemoryStorage) Term(idx int64) (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	offset := ms.offset()
	if idx < offset {
		return 0, ErrCompacted
	}
	if int(idx-offset) >= len(ms.entries) {
		return 0, ErrUnavailable
	}
	return ms.entries[idx-offset].Term, nil
}

func (ms *MemoryStorage) FirstIndex() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.firstIndex(), nil
}

// used in MemoryStorage to prevent reentrant lock
func (ms *MemoryStorage) firstIndex() int64 {
	return ms.offset() + 1
}

func (ms *MemoryStorage) LastIndex() (int64, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.lastIndex(), nil
}

// used in MemoryStorage to prevent reentrant lock
func (ms *MemoryStorage) lastIndex() int64 {
	return ms.offset() + int64(len(ms.entries)) - 1
}

// Compact discards all log entries prior to the idx
//
// EXAMPLE:
// before compact:
// Index [6 7 8 9]
// Term  [4 4 5 5]
// after compact (idx = 8):
// Index [8 9]
// Term  [5 5]
//
// NOTE:
// It is the application's responsibility to not attempt to compact an index greater than committed index
func (ms *MemoryStorage) Compact(idx int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if idx <= ms.offset() {
		return ErrCompacted
	}
	if idx > ms.lastIndex() {
		getLogger().Panic("compact index is out of bound", "idx", idx, "lastIndex", ms.lastIndex())
	}
	offset := idx - ms.offset()
	// compacted entries length == len(ms.entries) - offset
	entries := make([]rt.Entry, 1, int64(len(ms.entries))-offset)
	entries[0].Index = ms.entries[offset].Index
	entries[0].Term = ms.entries[offset].Term
	entries = append(entries, ms.entries[offset+1:]...)
	ms.entries = entries
	return nil
}

func (ms *MemoryStorage) CreateSnapshot(idx int64, cs *rt.ConfState, data []byte) (rt.Snapshot, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if idx <= ms.snapshot.Metadata.LastIndex {
		return rt.Snapshot{}, ErrSnapshotOutOfDate
	}
	if idx > ms.lastIndex() {
		getLogger().Panic("snapshot index is out of bound", "idx", idx, "lastIndex", ms.lastIndex())
	}

	offset := ms.offset()
	ms.snapshot.Metadata.LastIndex = idx
	ms.snapshot.Metadata.LastTerm = ms.entries[idx-offset].Term

	if cs != nil {
		ms.snapshot.Metadata.ConfState = cs
	}
	ms.snapshot.Data = data

	return ms.snapshot, nil
}

func (ms *MemoryStorage) Snapshot() (rt.Snapshot, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.snapshot, nil
}

func (ms *MemoryStorage) ApplySnapshot(snapshot rt.Snapshot) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	curr := ms.snapshot.Metadata.LastIndex
	incoming := snapshot.Metadata.LastIndex
	if curr >= incoming {
		return ErrSnapshotOutOfDate
	}

	ms.snapshot = snapshot
	// snapshot as dummy entry
	ms.entries = []rt.Entry{{
		Term:  snapshot.Metadata.LastTerm,
		Index: snapshot.Metadata.LastIndex,
	}}
	return nil
}

func (ms *MemoryStorage) Append(entries []rt.Entry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	// ensure entries are continuous
	if entries[0].Index <= ms.offset() {
		return ErrUnavailable
	}

	first, last := ms.firstIndex(), entries[0].Index+int64(len(entries))-1
	// return if new entries are all included in current entries
	if last < first {
		return nil
	}
	// truncate compacted entries
	// ensure compacted entries will not be append
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - ms.offset()
	switch {
	// normal case
	case int(offset) == len(ms.entries):
		ms.entries = append(ms.entries, entries...)
	// cover case
	//
	// NOTE:
	// incoming entries will overwrite current entries after the offset
	// use full slice expressions to protect entries at index (array index here) >= offset
	case int(offset) < len(ms.entries):
		ms.entries = append(ms.entries[:offset:offset], entries...)
	// missing entry case
	default:
		getLogger().Panic("missing log entry", "last", ms.lastIndex(), "append", entries[0].Index)
	}

	return nil
}
