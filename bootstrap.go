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

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

var emptyPersistentState = rt.PersistentState{}

var (
	ErrNonePeer        = errors.New("error none peer provided to boostrap")
	ErrNonemptyStorage = errors.New("error can not boostrap nonempty storage")
)

func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return ErrNonePeer
	}
	li, err := rn.raft.log.storage.LastIndex()
	if err != nil {
		return err
	}
	if li != 0 {
		return ErrNonemptyStorage
	}
	rn.prevPersistentState = emptyPersistentState
	rn.raft.becomeFollower(1, None)
	ccs := make([]rt.Entry, len(peers))
	for i, peer := range peers {
		cc := rt.ConfChange{
			Transaction: rt.ConfChangeTransition_Auto,
			Changes: []*rt.ConfChangeSingle{
				{
					Type:    rt.ConfChangeType_AddNode,
					NodeID:  peer.ID,
					NodeURL: peer.URL,
				},
			},
		}
		data, err := TMarshal(&cc)
		if err != nil {
			return err
		}

		ccs[i] = rt.Entry{
			Type:  rt.EntryType_Config,
			Term:  1,
			Index: int64(i) + 1, // start from 1
			Data:  data,
		}
	}
	rn.raft.log.appendUnstable(ccs...)

	// commit without quorum
	rn.raft.log.commitTo(int64(len(ccs)))
	for _, peer := range peers {
		rn.raft.applyConfChange(rt.ConfChange{
			Transaction: rt.ConfChangeTransition_Auto,
			Changes: []*rt.ConfChangeSingle{
				{
					Type:    rt.ConfChangeType_AddNode,
					NodeID:  peer.ID,
					NodeURL: peer.URL,
				},
			},
		})
	}

	return nil
}
