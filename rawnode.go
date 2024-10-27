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
	"context"
	"errors"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

var (
	ErrStepInternalMsg  = errors.New("error step internal message")
	ErrStepPeerNotFound = errors.New("error step peer not found")
)

// RawNode is only responsible for handing the logic of Raft
// It does not care about the message transport and other thing not related to the Raft core
type RawNode struct {
	raft *raft

	prevNodeState       *NodeState
	prevVolatileState   *VolatileState
	prevPersistentState rt.PersistentState

	messagesOnAdvance []rt.Message
}

func NewRawNode(c *Config) (*RawNode, error) {
	r := newRaft(c)
	rn := &RawNode{
		raft: r,
	}

	ns := r.nodeState()
	vs := r.volatileState()

	rn.prevNodeState = &ns
	rn.prevVolatileState = &vs
	rn.prevPersistentState = r.persistentState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick
func (rn *RawNode) Tick() {
	rn.raft.tick()
}

// Campaign start a leader election and become candidate
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(rt.Message{
		Type: rt.MessageType_Campaign,
	})
}

// Propose a command to be appended to raft log
func (rn *RawNode) Propose(data []byte) error {
	return rn.raft.Step(rt.Message{
		Type: rt.MessageType_Propose,
		Entries: EntryPointers([]rt.Entry{
			{Data: data},
		}),
	})
}

// ProposeConfChange propose config change to local node
func (rn *RawNode) ProposeConfChange(cc rt.ConfChange) error {
	data, err := TMarshal(context.Background(), &cc)
	if err != nil {
		return err
	}
	return rn.raft.Step(rt.Message{
		Type: rt.MessageType_Propose,
		Entries: []*rt.Entry{
			{
				Type: rt.EntryType_Config,
				Data: data,
			},
		},
	})
}

// ApplyConfChange apply config change to local node
func (rn *RawNode) ApplyConfChange(cc rt.ConfChange) *rt.ConfState {
	cs := rn.raft.applyConfChange(cc)
	return &cs
}

// Step advances the state machine with the given message
func (rn *RawNode) Step(msg rt.Message) error {
	if isInternalMessage(msg.Type) {
		return ErrStepInternalMsg
	}
	if _, ok := rn.raft.trk.progress[msg.Src]; !ok && isRPCRespMessage(msg.Type) {
		return ErrStepPeerNotFound
	}
	return rn.raft.Step(msg)
}

func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if ns := r.nodeState(); !ns.equal(rn.prevNodeState) {
		return true
	}
	if vs := r.volatileState(); !vs.equal(rn.prevVolatileState) {
		return true
	}
	if ps := r.persistentState(); !IsEmptyPersistentState(ps) && !IsPersistentStateEqual(ps, rn.prevPersistentState) {
		return true
	}
	if r.log.hasNextUnstableSnapshot() {
		return true
	}
	if len(r.messages) > 0 {
		return true
	}
	if r.log.hasNextUnstableEntries() || r.log.hasNextCommittedEntries() {
		return true
	}
	return false
}

func (rn *RawNode) Ready() Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	return rd
}

func (rn *RawNode) Advance() {
	for i, msg := range rn.messagesOnAdvance {
		_ = rn.raft.Step(msg)
		rn.messagesOnAdvance[i] = rt.Message{}
	}
	rn.resetMessagesOnAdvance()
}

func (rn *RawNode) Status() Status {
	return status(rn.raft)
}

func (rn *RawNode) BasicStatus() BasicStatus {
	return basicStatus(rn.raft)
}

func (rn *RawNode) readyWithoutAccept() Ready {
	r := rn.raft

	rd := Ready{
		Entries:          r.log.nextUnstableEntries(),
		CommittedEntries: r.log.nextCommittedEntries(),
		Messages:         r.messages,
	}
	if ns := r.nodeState(); !ns.equal(rn.prevNodeState) {
		// NOTE:
		// Equal to rd.NodeState = &ns, following style is for better read and maintain.
		//
		// escapingNodeState will escape to heap.
		escapingNodeState := ns
		rd.NodeState = &escapingNodeState
	}
	if vs := r.volatileState(); !vs.equal(rn.prevVolatileState) {
		escapingVolatileState := vs
		rd.VolatileState = &escapingVolatileState
	}
	if ps := r.persistentState(); !IsPersistentStateEqual(ps, rn.prevPersistentState) {
		rd.PersistentState = ps
	}
	if r.log.hasNextUnstableSnapshot() {
		rd.Snapshot = *r.log.nextUnstableSnapshot()
	}
	return rd
}

func (rn *RawNode) acceptReady(rd Ready) {
	r := rn.raft

	if rd.NodeState != nil {
		rn.prevNodeState = rd.NodeState
	}
	if rd.VolatileState != nil {
		rn.prevVolatileState = rd.VolatileState
	}
	if !IsEmptyPersistentState(rd.PersistentState) {
		rn.prevPersistentState = rd.PersistentState
	}

	if len(rn.messagesOnAdvance) != 0 {
		r.logger.Panicf("[%v] two accepted Ready without call to Advance", r.id)
	}
	rn.messagesOnAdvance = append(rn.messagesOnAdvance, r.localMessages...)
	if needMsgStorageAppend(r, rd) {
		msg := newMsgStorageAppend(r, rd.Snapshot)
		rn.messagesOnAdvance = append(rn.messagesOnAdvance, msg)
	}
	if needMsgStorageApply(rd) {
		msg := newMsgStorageApply(rd.CommittedEntries)
		rn.messagesOnAdvance = append(rn.messagesOnAdvance, msg)
	}

	r.messages = nil
	r.localMessages = nil

	r.log.acceptUnstable()
	n := len(rd.CommittedEntries)
	if n > 0 {
		idx := rd.CommittedEntries[n-1].Index
		r.log.acceptApplying(idx)
	}
}

func needMsgStorageAppend(r *raft, rd Ready) bool {
	return r.log.hasNextOrInProgressUnstableEntries() || !IsEmptySnapshot(rd.Snapshot)
}

func needMsgStorageApply(rd Ready) bool {
	return len(rd.CommittedEntries) > 0
}

func newMsgStorageAppend(r *raft, snapshot rt.Snapshot) rt.Message {
	msg := rt.Message{
		Type: rt.MessageType_StorageAppend,
	}
	if r.log.hasNextOrInProgressUnstableEntries() {
		msg.LogIndex = r.log.lastIndex()
		msg.LogTerm = r.log.lastTerm()
	}
	if !IsEmptySnapshot(snapshot) {
		escapingSnapshot := snapshot
		msg.Snapshot = &escapingSnapshot
	}
	return msg
}

func newMsgStorageApply(entries []rt.Entry) rt.Message {
	return rt.Message{
		Type:    rt.MessageType_StorageApply,
		Entries: EntryPointers(entries),
	}
}

func (rn *RawNode) resetMessagesOnAdvance() {
	rn.messagesOnAdvance = rn.messagesOnAdvance[:0]
}
