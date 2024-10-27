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

import rt "github.com/B1NARY-GR0UP/raft/raftthrift"

type Status struct {
	BasicStatus
	Config   ClusterConfig
	Progress map[int64]Progress
}

func status(r *raft) Status {
	s := Status{
		BasicStatus: basicStatus(r),
		Config:      r.trk.ClusterConfig.clone(),
	}
	if r.state == StateLeader {
		s.Progress = r.trk.progress.clone()
	}
	return s
}

type BasicStatus struct {
	ID int64
	NodeState

	VolatileState
	rt.PersistentState
}

func basicStatus(r *raft) BasicStatus {
	return BasicStatus{
		ID:              r.id,
		NodeState:       r.nodeState(),
		VolatileState:   r.volatileState(),
		PersistentState: r.persistentState(),
	}
}

// NodeState for rawnode.HasReady
type NodeState struct {
	LeaderID int64
	State    State
}

func (ns *NodeState) equal(state *NodeState) bool {
	return ns.LeaderID == state.LeaderID && ns.State == state.State
}

// VolatileState Volatile state on all servers
type VolatileState struct {
	CommitIndex int64
	LastApplied int64
}

func (vs *VolatileState) equal(state *VolatileState) bool {
	return vs.CommitIndex == state.CommitIndex && vs.LastApplied == state.LastApplied
}

func IsEmptyPersistentState(state rt.PersistentState) bool {
	return IsPersistentStateEqual(state, rt.PersistentState{})
}

func IsPersistentStateEqual(a, b rt.PersistentState) bool {
	return a.CurrentTerm == b.CurrentTerm && a.VotedFor == b.VotedFor
}
