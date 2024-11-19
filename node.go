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
	"fmt"
	"strings"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

var _ StateMachine = (*Node)(nil)

type StateMachine any

type Node interface {
	// StateMachine
	// Raft is essentially a state machine
	StateMachine
	// Status return the status of the Raft state machine
	Status() Status
	// Ready is an interface to receive command from the Raft module
	// after retrieving the state returned by Ready, call Advance
	//
	// e.g. persistence Raft log, send RPC request
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready
	// Advance notifies the Node that the application has saved progress up to the last Ready
	// It prepares the node to return the next available Ready
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	//
	// e.g. When followers receives RPC requests from leader, it will submit the messages to the Raft module through Step
	// the user only responsible for the message transport through network
	Step(ctx context.Context, msg rt.Message) error
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log
	//
	// NOTE: Proposals can be lost without notice, therefore it is user's job to ensure proposal retries
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc rt.ConfChange) error
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	ApplyConfChange(cc rt.ConfChange) *rt.ConfState
	// Tick increments the internal logical clock for this Node. Election timeouts
	// and heartbeat timeouts are in units of ticks.
	Tick()
	// Stop the node immediately
	Stop()
}

var ErrStopped = errors.New("error raft stopped")

const _tickCBufferSize = 128

type node struct {
	rn *RawNode

	propC      chan messageWithResult
	recvC      chan rt.Message
	confApplyC chan rt.ConfChange
	confStateC chan rt.ConfState

	readyC  chan Ready
	statusC chan chan Status

	tickC    chan struct{}
	advanceC chan struct{}
	stopC    chan struct{}

	done chan struct{}
}

type messageWithResult struct {
	msg    rt.Message
	result chan error
}

func newNode(rn *RawNode) *node {
	return &node{
		rn: rn,

		propC:      make(chan messageWithResult),
		recvC:      make(chan rt.Message),
		confApplyC: make(chan rt.ConfChange),
		confStateC: make(chan rt.ConfState),

		readyC:  make(chan Ready),
		statusC: make(chan chan Status),

		tickC:    make(chan struct{}, _tickCBufferSize),
		advanceC: make(chan struct{}),
		stopC:    make(chan struct{}),

		done: make(chan struct{}),
	}
}

func (n *node) Status() Status {
	ch := make(chan Status)
	select {
	case n.statusC <- ch:
		return <-ch
	case <-n.done:
	}
	return Status{}
}

func (n *node) Ready() <-chan Ready {
	return n.readyC
}

func (n *node) Advance() {
	select {
	case n.advanceC <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) Step(ctx context.Context, msg rt.Message) error {
	return n.step(ctx, msg)
}

func (n *node) Campaign(ctx context.Context) error {
	return n.step(ctx, rt.Message{
		Type: rt.MessageType_Campaign,
	})
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, rt.Message{
		Type: rt.MessageType_Propose,
		Entries: EntryPointers([]rt.Entry{
			{
				Data: data,
			},
		}),
	})
}

func (n *node) ProposeConfChange(ctx context.Context, cc rt.ConfChange) error {
	data, err := TMarshal(&cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, rt.Message{
		Type: rt.MessageType_Propose,
		Entries: []*rt.Entry{
			{
				Type: rt.EntryType_Config,
				Data: data,
			},
		},
	})
}

func (n *node) ApplyConfChange(cc rt.ConfChange) *rt.ConfState {
	var cs rt.ConfState
	select {
	case n.confApplyC <- cc:
	case <-n.done:
	}
	select {
	case cs = <-n.confStateC:
	case <-n.done:
	}
	return &cs
}

func (n *node) Tick() {
	select {
	case n.tickC <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warnf("[%v] a tick missed to fire, node blocks too long", n.rn.raft.id)
	}
}

func (n *node) Stop() {
	select {
	case n.stopC <- struct{}{}:
	case <-n.done:
		return
	}
	// block until the stop signal has been acknowledged by run()
	<-n.done
}

func (n *node) step(ctx context.Context, msg rt.Message) error {
	return n.stepWithWaitOption(ctx, msg, false)
}

func (n *node) stepWait(ctx context.Context, msg rt.Message) error {
	return n.stepWithWaitOption(ctx, msg, true)
}

func (n *node) stepWithWaitOption(ctx context.Context, msg rt.Message, wait bool) error {
	if msg.Type != rt.MessageType_Propose {
		select {
		case n.recvC <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	propMsg := messageWithResult{
		msg: msg,
	}
	if wait {
		propMsg.result = make(chan error, 1)
	}
	select {
	case n.propC <- propMsg:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	// wait for propose result
	select {
	case err := <-propMsg.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *node) run() {
	r := n.rn.raft

	var propC chan messageWithResult
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	leaderID := None

	for {
		if advanceC == nil && n.rn.HasReady() {
			rd = n.rn.readyWithoutAccept()
			readyC = n.readyC
		}

		if leaderID != r.leaderID {
			if r.hasLeader() {
				if leaderID == None {
					r.logger.Infof("[%v] raft node elected a leader [%v] at term %v", r.id, r.leaderID, r.currentTerm)
				} else {
					r.logger.Infof("[%v] raft node change leader from %v to %v at term %v", r.id, leaderID, r.leaderID, r.currentTerm)
				}
				propC = n.propC
			} else {
				r.logger.Infof("[%v] raft node lost leader [%v] at term %v", r.id, leaderID, r.currentTerm)
				propC = nil
			}
			leaderID = r.leaderID
		}

		select {
		case propMsg := <-propC:
			err := n.rn.Step(propMsg.msg)
			if propMsg.result != nil {
				propMsg.result <- err
				close(propMsg.result)
			}
		case msg := <-n.recvC:
			_ = n.rn.Step(msg)
		case cc := <-n.confApplyC:
			_, okBefore := r.trk.progress[r.id]
			cs := r.applyConfChange(cc)
			// current node not in the latest config
			if _, okAfter := r.trk.progress[r.id]; okBefore && !okAfter {
				var found bool
				for _, sli := range [][]int64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sli {
						if id == r.id {
							found = true
							break
						}
					}
					if found {
						break
					}
				}
				// avoid node with old config receive proposal
				if !found {
					n.propC = nil
				}
			}
			select {
			case n.confStateC <- cs:
			case <-n.done:
			}
		case <-n.tickC:
			n.rn.Tick()
		case readyC <- rd:
			n.rn.acceptReady(rd)
			advanceC = n.advanceC
			readyC = nil
		case <-advanceC:
			n.rn.Advance()
			rd = Ready{}
			advanceC = nil
		case ch := <-n.statusC:
			ch <- status(r)
		case <-n.stopC:
			close(n.done)
			return
		}
	}
}

type Ready struct {
	*NodeState
	*VolatileState
	rt.PersistentState

	Entries          []rt.Entry
	CommittedEntries []rt.Entry

	Messages []rt.Message

	Snapshot rt.Snapshot
}

func (r Ready) String() string {
	var sb strings.Builder
	sb.WriteString("Ready {\n")
	sb.WriteString(fmt.Sprintf("  NodeState: %+v,\n", r.NodeState))
	sb.WriteString(fmt.Sprintf("  VolatileState: %+v,\n", r.VolatileState))
	sb.WriteString(fmt.Sprintf("  PersistentState: %+v,\n", r.PersistentState))
	sb.WriteString(fmt.Sprintf("  Entries: %+v,\n", r.Entries))
	sb.WriteString(fmt.Sprintf("  CommittedEntries: %+v,\n", r.CommittedEntries))
	sb.WriteString(fmt.Sprintf("  Messages: %+v,\n", r.Messages))
	sb.WriteString(fmt.Sprintf("  Snapshot: %+v,\n", r.Snapshot))
	sb.WriteString("}")
	return sb.String()
}

type Peer struct {
	ID  int64
	URL []byte
}

// StartNode start a raft node
func StartNode(c *Config, peers []Peer) Node {
	n := setupNode(c, peers)
	go n.run()
	return n
}

// RestartNode restart a raft node
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return n
}

func setupNode(c *Config, peers []Peer) *node {
	if len(peers) == 0 {
		panic("peers is not provided; use RestartNode instead")
	}
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warnf("error occurred during starting a new node: %v", err)
	}
	return newNode(rn)
}
