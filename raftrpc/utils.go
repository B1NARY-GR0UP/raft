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

package raftrpc

import (
	rr "github.com/B1NARY-GR0UP/raft/raftrpc/kitex_gen/raft"
	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

func RPCMsg(msg rt.Message) rr.Message {
	// convert entries
	var entries []*rr.Entry
	if msg.Entries != nil {
		entries = make([]*rr.Entry, len(msg.Entries))
		for i, entry := range msg.Entries {
			entries[i] = &rr.Entry{
				Type:  rr.EntryType(entry.Type),
				Term:  entry.Term,
				Index: entry.Index,
				Data:  entry.Data,
			}
		}
	}
	// convert snapshot
	var snapshot *rr.Snapshot
	if msg.Snapshot != nil && msg.Snapshot.Metadata != nil {
		snapshot = &rr.Snapshot{
			Metadata: &rr.SnapshotMetadata{
				ConfState: &rr.ConfState{
					Voters: msg.Snapshot.Metadata.ConfState.Voters,
				},
				LastIndex: msg.Snapshot.Metadata.LastIndex,
				LastTerm:  msg.Snapshot.Metadata.LastTerm,
			},
			Data: msg.Snapshot.Data,
		}
	}
	return rr.Message{
		Type:         rr.MessageType(msg.Type),
		Src:          msg.Src,
		Dst:          msg.Dst,
		Term:         msg.Term,
		LogIndex:     msg.LogIndex,
		LogTerm:      msg.LogTerm,
		Entries:      entries,
		LeaderCommit: msg.LeaderCommit,
		Snapshot:     snapshot,
		Reject:       msg.Reject,
	}
}

func RaftMsg(msg rr.Message) rt.Message {
	// convert entries
	var entries []*rt.Entry
	if msg.Entries != nil {
		entries = make([]*rt.Entry, len(msg.Entries))
		for i, entry := range msg.Entries {
			entries[i] = &rt.Entry{
				Type:  rt.EntryType(entry.Type),
				Term:  entry.Term,
				Index: entry.Index,
				Data:  entry.Data,
			}
		}
	}
	// convert snapshot
	var snapshot *rt.Snapshot
	if msg.Snapshot != nil && msg.Snapshot.Metadata != nil {
		snapshot = &rt.Snapshot{
			Metadata: &rt.SnapshotMetadata{
				ConfState: &rt.ConfState{
					Voters: msg.Snapshot.Metadata.ConfState.Voters,
				},
				LastIndex: msg.Snapshot.Metadata.LastIndex,
				LastTerm:  msg.Snapshot.Metadata.LastTerm,
			},
			Data: msg.Snapshot.Data,
		}
	}
	return rt.Message{
		Type:         rt.MessageType(msg.Type),
		Src:          msg.Src,
		Dst:          msg.Dst,
		Term:         msg.Term,
		LogIndex:     msg.LogIndex,
		LogTerm:      msg.LogTerm,
		Entries:      entries,
		LeaderCommit: msg.LeaderCommit,
		Snapshot:     snapshot,
		Reject:       msg.Reject,
	}
}
