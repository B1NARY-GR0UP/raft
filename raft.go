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
	"math"
	"strings"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

// None should not be used as a simply number zero
const (
	None     int64 = 0
	Infinite int64 = math.MaxInt64
)

type State int64

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

var (
	ErrProposalDropped = errors.New("error drop proposal")
	ErrEmptySnapshot   = errors.New("error empty snapshot")
	ErrRestoreConfig   = errors.New("error restore config")
)

type raft struct {
	id       int64
	leaderID int64

	// Follower | Candidate | Leader
	state State

	// Persistent state on all servers
	currentTerm int64
	votedFor    int64
	// raft log
	log *log

	trk *tracker

	tick tickFunc
	step stepFunc

	// NOTE: according to the paper, Raft only have one election timeout (electionTimeout)
	// heartbeatTimeout means the number of tick needed to represent one heartbeat
	// in other words, if the number exceed the heartbeatTimeout which means that timeout occurs
	//
	// in short, heartbeatTimeout is the heartbeat interval
	heartbeatTimeout int
	electionTimeout  int

	// number of ticks since it reached last heartbeatTimeout
	// only leader keeps heartbeatElapsed
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout when it is leader or candidate
	//
	// number of ticks since it reached last electionTimeout or received a valid message
	// from current leader when it is a follower
	electionElapsed int

	// typically 100-500 ms
	// [electionTimeout, 2 * electionTimeout)
	randomizeElectionTimeout int

	// messages that need to send to other nodes (exclude itself)
	// we assume that all the messages are sync (append to stable storage then advance)
	messages []rt.Message
	// localMessages msg.Dst == r.id that need to add to stepOnAdvance
	// localMessages are messages which dst is node itself
	localMessages []rt.Message

	// only one conf change may be pending (in the log, but not yet
	// applied) at a time.
	// This is enforced via pendingConfIndex,
	// which is set to a value >= the log index of the latest pending configuration change (if any).
	// Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this value
	pendingConfIndex int64

	logger Logger
}

type (
	tickFunc func()
	stepFunc func(msg rt.Message) error
)

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err)
	}

	// raft log
	log := newLog(c.Storage, c.Logger)
	// read persistent state and cluster configuration from storage
	ps, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &raft{
		id:               c.ID,
		leaderID:         None,
		currentTerm:      0, // initialized to 0 on first boot, increases monotonically
		votedFor:         None,
		log:              log,
		trk:              newTracker(),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		logger:           c.Logger,
	}

	// restore from conf state
	cfg, pm, err := restore(changer{
		trk:       *r.trk,
		lastIndex: log.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStateEqual(cs, r.switchToConfig(cfg, pm))
	// restore from persistent state
	if !IsEmptyPersistentState(ps) {
		r.loadPersistentState(ps)
	}

	// apply to last applied index if restart
	if c.LastApplied > 0 {
		log.applyTo(c.LastApplied)
	}
	// initialize all node into follower
	r.becomeFollower(r.currentTerm, None)

	// collect voter ids for log output
	var nodes []string
	for _, id := range r.trk.voterIDs() {
		nodes = append(nodes, fmt.Sprintf("%x", id))
	}
	r.logger.Infof("%x new raft [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodes, ","), r.currentTerm, r.log.commitIndex, r.log.lastApplied, r.log.lastIndex(), r.log.lastTerm())
	return r
}

func (r *raft) hasLeader() bool {
	return r.leaderID != None
}

func (r *raft) nodeState() NodeState {
	return NodeState{
		LeaderID: r.leaderID,
		State:    r.state,
	}
}

func (r *raft) volatileState() VolatileState {
	return VolatileState{
		CommitIndex: r.log.commitIndex,
		LastApplied: r.log.lastApplied,
	}
}

func (r *raft) persistentState() rt.PersistentState {
	return rt.PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
	}
}

func (r *raft) loadPersistentState(ps rt.PersistentState) {
	r.currentTerm = ps.CurrentTerm
	r.votedFor = ps.VotedFor
}

// Step handle public logic for messages
// stepLeader, stepCandidate, stepFollower focus on respective details
func (r *raft) Step(msg rt.Message) error {
	// THESIS:
	// Rules for Servers
	// All servers:
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if isRPCReqMessage(msg.Type) || isRPCRespMessage(msg.Type) {
		if msg.Term > r.currentTerm {
			r.logger.Infof("[%v] node received message [type: %v, src: %v, term: %v] at term %v",
				r.id, msg.Type, msg.Src, msg.Term, r.currentTerm)
			r.becomeFollower(msg.Term, msg.Src)
		}
	}

	switch msg.Type {
	case rt.MessageType_StorageAppend:
		if msg.LogIndex != 0 {
			r.log.stableTo(msg.LogIndex, msg.LogTerm)
		}
		if msg.Snapshot != nil {
			r.applySnapshot(msg.Snapshot)
		}
	case rt.MessageType_StorageApply:
		if len(msg.Entries) > 0 {
			idx := msg.Entries[len(msg.Entries)-1].Index
			r.applyTo(idx)
		}
	}

	if err := r.step(msg); err != nil {
		return err
	}

	return nil
}

func (r *raft) restore(snap rt.Snapshot) bool {
	if snap.Metadata.LastIndex <= r.log.commitIndex {
		return false
	}
	// current node is already up-to-date with the snapshot, no need to restore from snapshot
	if r.log.matchTerm(snap.Metadata.LastIndex, snap.Metadata.LastTerm) {
		r.logger.Infof("[%v] node [commitIndex: %d, lastIndex: %d, lastTerm: %d] fast-forwarded commit to snapshot [lastIndex: %d, lastTerm: %d]",
			r.id, r.log.commitIndex, r.log.lastIndex(), r.log.lastTerm(), snap.Metadata.LastIndex, snap.Metadata.LastTerm)
		r.log.commitTo(snap.Metadata.LastIndex)
		return false
	}

	r.log.restore(snap)
	// reset cluster configuration
	r.trk = newTracker()
	cfg, pm, err := restore(changer{
		trk:       *r.trk,
		lastIndex: snap.Metadata.LastIndex,
	}, *snap.Metadata.ConfState)
	if err != nil {
		panic(ErrRestoreConfig)
	}
	assertConfStateEqual(*snap.Metadata.ConfState, r.switchToConfig(cfg, pm))

	r.logger.Infof("[%v] node [commitIndex: %d, lastIndex: %d, lastTerm: %d] restored snapshot [lastIndex: %d, lastTerm: %d]",
		r.id, r.log.commitIndex, r.log.lastIndex(), r.log.lastTerm(), snap.Metadata.LastIndex, snap.Metadata.LastTerm)
	return true
}

// reset raft to input term
func (r *raft) reset(term int64) {
	if r.currentTerm != term {
		r.currentTerm = term
		r.votedFor = None
	}
	r.leaderID = None

	r.resetElectionTimeout()
	r.resetHeartbeatTimeout()
	r.resetRandomizedElectionTimeout()

	r.trk.resetVotes()
	// THESIS:
	// Volatile state on leaders
	// NextIndex[] and MatchIndex[] need to reinitialized after election
	r.trk.visitProgress(func(id int64, p *Progress) {
		// initialized to 0, increases monotonically
		p.MatchIndex = 0
		// If the id match the node itself, the matchIndex should be the latest log index of its raft log
		// Same to nextIndex, only leader uses matchIndex, the leader's raft log is always up-to-date
		if r.id == id {
			p.MatchIndex = r.log.lastIndex()
		}
		// initialized to leader last log index + 1
		// although the server might not be a leader, only leader use nextIndex, so it's ok not to make a judgement
		p.NextIndex = r.log.lastIndex() + 1
	})

	r.pendingConfIndex = 0
}

// pastElectionTimeout check if election timeout occurred
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizeElectionTimeout
}

// resetElectionTimeout reset current node's election elapsed
func (r *raft) resetElectionTimeout() {
	r.electionElapsed = 0
}

// resetRandomizedElectionTimeout reset current node's election timout randomly
func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizeElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// pastHeartbeatTimeout check if heartbeat timeout occurred
func (r *raft) pastHeartbeatTimeout() bool {
	return r.heartbeatElapsed >= r.heartbeatTimeout
}

// resetHeartbeatTimeout reset current node's heartbeat timeout
func (r *raft) resetHeartbeatTimeout() {
	r.heartbeatElapsed = 0
}

// becomeFollower convert current node to follower
func (r *raft) becomeFollower(currentTerm, leaderID int64) {
	r.step = r.stepFollower
	r.reset(currentTerm)
	r.tick = r.tickElection
	r.leaderID = leaderID
	r.state = StateFollower
	r.logger.Infof("[%v] node became folower at term %v", r.id, r.currentTerm)
}

// becomeCandidate convert current node to candidate
func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition: leader => candidate")
	}
	r.step = r.stepCandidate
	r.reset(r.currentTerm + 1)
	r.tick = r.tickElection
	r.votedFor = r.id
	r.state = StateCandidate
	r.logger.Infof("[%v] node became candidate at term %v", r.id, r.currentTerm)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition: follower => leader")
	}
	r.step = r.stepLeader
	r.reset(r.currentTerm)
	r.tick = r.tickHeartbeat
	r.leaderID = r.id
	r.state = StateLeader
	r.pendingConfIndex = r.log.lastIndex()
	// send empty entry in order to commit entries from previous term
	r.appendEntries(rt.Entry{
		Data: nil,
	})
	r.logger.Infof("[%v] node became leader at term %v", r.id, r.currentTerm)
}

// NOTE: must not call stepFollower directly, call step instead
func (r *raft) stepFollower(msg rt.Message) error {
	switch msg.Type {
	case rt.MessageType_Propose:
		if r.leaderID == None {
			return ErrProposalDropped
		}
		msg.Dst = r.leaderID
		r.send(msg)
	case rt.MessageType_Campaign:
		r.campaign()
	case rt.MessageType_RequestVote:
		r.handleRequestVote(msg)
	case rt.MessageType_AppendEntries:
		r.resetElectionTimeout()
		r.leaderID = msg.Src
		r.handleAppendEntries(msg)
	case rt.MessageType_InstallSnapshot:
		r.resetElectionTimeout()
		r.leaderID = msg.Src
		r.handleInstallSnapshot(msg)
	}
	return nil
}

// NOTE: must not call stepCandidate directly, call step instead
func (r *raft) stepCandidate(msg rt.Message) error {
	switch msg.Type {
	case rt.MessageType_Propose:
		r.logger.Infof("[%v] no leader at term %v, dropping proposal", r.id, r.currentTerm)
		return ErrProposalDropped
	case rt.MessageType_Campaign:
		r.campaign()
	case rt.MessageType_RequestVote:
		r.handleRequestVote(msg)
	case rt.MessageType_RequestVoteResp:
		granted, rejected, result := r.poll(msg.Src, !msg.Reject)
		r.logger.Infof("[%v] node receives votes [granted: %v, rejected: %v] at term %v", r.id, granted, rejected, r.currentTerm)
		switch result {
		case VoteWon:
			// become leader
			r.becomeLeader()
			// broadcast empty log entry in order to commit entries from previous term
			r.broadcastAppendEntries(false)
		case VoteLost:
			r.becomeFollower(r.currentTerm, None)
		case VotePending:
			// wait for more votes
		default:
			panic("invalid vote result")
		}
	case rt.MessageType_AppendEntries:
		if msg.Term == r.currentTerm {
			r.becomeFollower(msg.Term, msg.Src)
		}
		r.handleAppendEntries(msg)
	case rt.MessageType_InstallSnapshot:
		if msg.Term == r.currentTerm {
			r.becomeFollower(msg.Term, msg.Src)
		}
		r.handleInstallSnapshot(msg)
	}
	return nil
}

// NOTE: must not call stepLeader directly, call step instead
func (r *raft) stepLeader(msg rt.Message) error {
	switch msg.Type {
	case rt.MessageType_Beat:
		r.broadcastAppendEntries(true)
	case rt.MessageType_Propose:
		if len(msg.Entries) == 0 {
			r.logger.Panicf("[%v] empty entries received from peer %v", r.id, msg.Src)
		}
		if _, ok := r.trk.progress[r.id]; !ok {
			return ErrProposalDropped
		}
		for i, entry := range msg.Entries {
			var hasConfChange bool
			var cc rt.ConfChange
			if entry.Type == rt.EntryType_Config {
				hasConfChange = true
				if err := TUnmarshal(entry.Data, &cc); err != nil {
					panic(err)
				}
			}
			if hasConfChange {
				alreadyPending := r.pendingConfIndex > r.log.lastApplied
				alreadyJoint := len(r.trk.Voters[1]) > 0
				wantsLeaveJoint := len(cc.Changes) == 0

				// check config change
				var failedCheck string
				if alreadyPending {
					failedCheck = fmt.Sprintf("[%v] possible unapplied conf change at index %d (applied to %d)", r.id, r.pendingConfIndex, r.log.lastApplied)
				} else if alreadyJoint && !wantsLeaveJoint {
					failedCheck = fmt.Sprintf("[%v] must quit joint config first", r.id)
				} else if !alreadyJoint && wantsLeaveJoint {
					failedCheck = fmt.Sprintf("[%v] not in joint state, can not quit", r.id)
				}

				if failedCheck != "" {
					r.logger.Infof("[%v] ignoring conf change %v at config %v: %s", r.id, cc, r.trk.ClusterConfig, failedCheck)
					msg.Entries[i] = &rt.Entry{
						Type: rt.EntryType_Normal,
					}
				} else {
					r.pendingConfIndex = r.log.lastIndex() + int64(i) + 1
				}
			}
		}

		r.appendEntries(EntryValues(msg.Entries)...)
		r.broadcastAppendEntries(false)
	case rt.MessageType_AppendEntriesResp:
		lp, ok := r.trk.progress[msg.Src]
		if !ok {
			r.logger.Panicf("[%v] no log progress available for peer %v", r.id, msg.Src)
		}
		// THESIS:
		// Rules for Servers
		// Leaders:
		// - If successful: update nextIndex and matchIndex for follower
		// - If AppendEntries fails because of log inconsistency:
		//   decrement nextIndex and retry
		if msg.Reject {
			lp.decreaseNextIndex(1)
			r.sendAppendEntries(msg.Src, lp.NextIndex, false)
		} else {
			lp.update(r.log.lastIndex())
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N
			r.leaderCommit(r.trk.commitIndex())
		}
	case rt.MessageType_InstallSnapshotResp:
		// update term in Step
	}
	return nil
}

// tickElection is run by followers and candidates to check whether to start an election
func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() || r.state == StateFollower && r.votedFor == None {
		// reset election timeout to prepare the next election timeout when current node is candidate
		r.resetElectionTimeout()
		if err := r.Step(rt.Message{
			Type: rt.MessageType_Campaign,
		}); err != nil {
			r.logger.Debugf("[%v] election error: %v", r.id, err.Error())
		}
	}
}

// tickHeartbeat is run by leader to check whether to send heartbeat message to followers
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.state != StateLeader {
		return
	}
	if r.pastHeartbeatTimeout() {
		r.resetHeartbeatTimeout()
		if err := r.Step(rt.Message{
			Type: rt.MessageType_Beat,
		}); err != nil {
			r.logger.Debugf("[%v] heartbeat error: %v", r.id, err.Error())
		}
	}
}

// handleAppendEntries
//
// THESIS:
// AppendEntries RPC Receiver implementation
//
// If desired, the protocol can be optimized to reduce the
// number of rejected AppendEntries RPCs.
// For example,
// when rejecting an AppendEntries request, the follower
// can include the term of the conflicting entry and the first
// index it stores for that term. With this information, the
// leader can decrement nextIndex to bypass all of the conflicting entries in that term;
// one AppendEntries RPC will be required for each term with conflicting entries,
// rather than one RPC per entry.
// In practice, we doubt this optimization is necessary,
// since failures happen infrequently and it is unlikely that there will be many inconsistent entries.
func (r *raft) handleAppendEntries(msg rt.Message) {
	// 1. Reply false if term < currentTerm
	if msg.Term < r.currentTerm {
		r.send(rt.Message{
			Type:   rt.MessageType_AppendEntriesResp,
			Dst:    msg.Src,
			Term:   r.currentTerm,
			Reject: true,
		})
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if !r.log.matchTerm(msg.LogIndex, msg.LogTerm) {
		r.send(rt.Message{
			Type:   rt.MessageType_AppendEntriesResp,
			Dst:    msg.Src,
			Term:   r.currentTerm,
			Reject: true,
		})
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	//
	// 4. Append any new entries not already in the log
	//
	// NOTE: heartbeat will skip this step
	r.log.append(msg.LogIndex, EntryValues(msg.Entries)...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	r.log.commitTo(msg.LeaderCommit)
	r.send(rt.Message{
		Type: rt.MessageType_AppendEntriesResp,
		Dst:  msg.Src,
		Term: r.currentTerm,
	})
}

// handleRequestVote
//
// THESIS:
// RequestVote RPC Receiver implementation
func (r *raft) handleRequestVote(msg rt.Message) {
	// 1. Reply false if term < currentTerm
	if msg.Term < r.currentTerm {
		r.send(rt.Message{
			Type:   rt.MessageType_RequestVoteResp,
			Dst:    msg.Src,
			Term:   r.currentTerm,
			Reject: true,
		})
		return
	}
	// 2. If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if !r.canVote(msg.Src, msg.LogIndex, msg.LogTerm) {
		r.logger.Infof("[%v] node rejects vote from candidate %v at term %v, voteFor %v, log [lastTerm: %v, lastIndex: %v], msg [term: %v, index: %v]",
			r.id, msg.Src, r.currentTerm, r.votedFor, r.log.lastTerm(), r.log.lastIndex(), msg.LogTerm, msg.LogIndex)
		r.send(rt.Message{
			Type:   rt.MessageType_RequestVoteResp,
			Dst:    msg.Src,
			Term:   r.currentTerm,
			Reject: true,
		})
		return
	}
	// grant vote
	r.logger.Infof("[%v] node votes for candidate %v at term %v, voteFor %v, log [lastTerm: %v, lastIndex: %v], msg [term: %v, index: %v]",
		r.id, msg.Src, r.currentTerm, r.votedFor, r.log.lastTerm(), r.log.lastIndex(), msg.LogTerm, msg.LogIndex)
	r.send(rt.Message{
		Type: rt.MessageType_RequestVoteResp,
		Dst:  msg.Src,
		Term: r.currentTerm,
	})
	r.resetElectionTimeout()
	r.votedFor = msg.Src
}

func (r *raft) handleInstallSnapshot(msg rt.Message) {
	// reply immediately if term < currentTerm
	if msg.Term < r.currentTerm {
		r.send(rt.Message{
			Type: rt.MessageType_InstallSnapshotResp,
			Dst:  msg.Src,
			Term: r.currentTerm,
		})
		return
	}
	var snap rt.Snapshot
	if msg.Snapshot != nil {
		snap = *msg.Snapshot
	}
	// reset state machine using snapshot contents (and load snapshot's cluster configuration)
	if r.restore(snap) {
		r.logger.Infof("[%v] node [commit: %d] restored snapshot [lastIndex: %d, lastTerm: %d]", r.id, r.log.commitIndex, snap.Metadata.LastIndex, snap.Metadata.LastTerm)
		r.send(rt.Message{
			Type: rt.MessageType_InstallSnapshotResp,
			Dst:  msg.Src,
			Term: r.currentTerm,
		})
		return
	}
	r.logger.Infof("[%v] node [commit: %d] ignored snapshot [lastIndex: %d, lastTerm: %d]", r.id, r.log.commitIndex, snap.Metadata.LastIndex, snap.Metadata.LastTerm)
	r.send(rt.Message{
		Type: rt.MessageType_InstallSnapshotResp,
		Dst:  msg.Src,
		Term: r.currentTerm,
	})
}

func (r *raft) broadcastAppendEntries(heartbeat bool) {
	r.trk.visitProgress(func(id int64, p *Progress) {
		if id == r.id {
			return
		}
		r.sendAppendEntries(id, p.NextIndex, heartbeat)
	})
}

func (r *raft) broadcastRequestVote() {
	r.trk.visitVoters(func(id int64) {
		if id == r.id {
			return
		}
		r.sendRequestVote(id)
	})
}

// self RequestVote
func (r *raft) requestVote() {
	r.send(rt.Message{
		Type:   rt.MessageType_RequestVoteResp,
		Dst:    r.id,
		Term:   r.currentTerm,
		Reject: false,
	})
}

// self AppendEntries
func (r *raft) appendEntries(entries ...rt.Entry) {
	// set raft log term and index
	li := r.log.lastIndex()
	for i := range entries {
		entries[i].Term = r.currentTerm
		entries[i].Index = li + int64(i) + 1
	}

	r.log.appendUnstable(entries...)
	r.send(rt.Message{
		Type:   rt.MessageType_AppendEntriesResp,
		Dst:    r.id,
		Term:   r.currentTerm,
		Reject: false,
	})
}

func (r *raft) sendAppendEntries(dst, low int64, heartbeat bool) {
	// index of log entry immediately preceding new ones
	prevLogIndex := low - 1
	// term of prevLogIndex entry
	prevLogTerm, err := r.log.term(prevLogIndex)
	if err != nil {
		// the log is truncated, send snapshot instead
		r.sendInstallSnapshot(dst)
		return
	}
	var entries []rt.Entry
	if !heartbeat {
		entries, err = r.log.entriesFrom(low)
		if err != nil {
			r.sendInstallSnapshot(dst)
			return
		}
	}
	if !heartbeat {
		r.logger.Infof("[%v] node send AppendEntries RPC (with entries) at term %v, msg [src: %v, dst: %v, logIndex: %v, logTerm: %v]",
			r.id, r.currentTerm, r.id, dst, prevLogIndex, prevLogTerm)
	}
	r.send(rt.Message{
		Type:         rt.MessageType_AppendEntries,
		Dst:          dst,
		Term:         r.currentTerm,
		LogIndex:     prevLogIndex,
		LogTerm:      prevLogTerm,
		Entries:      EntryPointers(entries),
		LeaderCommit: r.log.commitIndex,
	})
}

func (r *raft) sendRequestVote(dst int64) {
	r.logger.Infof("[%v] node send RequestVote RPC at term %v, msg [src: %v, dst: %v]", r.id, r.currentTerm, r.id, dst)
	r.send(rt.Message{
		Type:     rt.MessageType_RequestVote,
		Dst:      dst,
		Term:     r.currentTerm,
		LogIndex: r.log.lastIndex(),
		LogTerm:  r.log.lastTerm(),
	})
}

func (r *raft) sendInstallSnapshot(dst int64) {
	snap, err := r.log.snapshot()
	if err != nil {
		panic(err)
	}
	if IsEmptySnapshot(snap) {
		panic(ErrEmptySnapshot)
	}
	r.logger.Infof("[%v] node send InstallSnapshot RPC at term %v, msg [src: %v, dst: %v]", r.id, r.currentTerm, r.id, dst)
	r.send(rt.Message{
		Type:     rt.MessageType_InstallSnapshot,
		Dst:      dst,
		Term:     r.currentTerm,
		Snapshot: &snap,
	})
}

// leaderCommit
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (r *raft) leaderCommit(commitIndex int64) bool {
	if r.state != StateLeader {
		return false
	}
	N := commitIndex
	if N > r.log.commitIndex &&
		r.log.zeroTermOnOutOfBounds(r.log.term(N)) == r.currentTerm &&
		r.currentTerm != 0 {
		r.log.commitTo(N)
		return true
	}
	return false
}

func (r *raft) poll(id int64, voteGranted bool) (granted, rejected int64, _ VoteResult) {
	if voteGranted {
		r.logger.Infof("[%v] vote granted from %v at term %v", r.id, id, r.currentTerm)
	} else {
		r.logger.Infof("[%v] vote rejected from %v at term %v", r.id, id, r.currentTerm)
	}
	r.trk.recordVote(id, voteGranted)
	return r.trk.tallyVotes()
}

func (r *raft) send(msg rt.Message) {
	if msg.Src == None {
		msg.Src = r.id
	}
	if msg.Term == None {
		msg.Term = r.currentTerm
	}
	if msg.Dst == r.id {
		r.localMessages = append(r.localMessages, msg)
		return
	}
	r.messages = append(r.messages, msg)
}

// promotable check if the node itself can be promoted to a leader
func (r *raft) promotable() bool {
	_, ok := r.trk.progress[r.id]
	return ok && !r.log.hasNextOrInProgressSnapshot()
}

func (r *raft) campaign() {
	if r.state == StateLeader {
		r.logger.Debugf("[%v] node ignore canpaign because already leader", r.id)
		return
	}
	if !r.promotable() {
		r.logger.Warnf("[%v] node can not campaign because is not promotable", r.id)
		return
	}

	r.logger.Infof("[%v] node starting a new leader election at term %v", r.id, r.currentTerm)
	r.becomeCandidate()

	r.requestVote()
	r.broadcastRequestVote()
}

// canVote
//
// THESIS:
// RequestVote RPC
// Receiver implementation
// 2. If votedFor is null or candidateID, and candidate's log is at least as up-to-date as receiver's log, grant vote
func (r *raft) canVote(candidateID, lastIndex, lastTerm int64) bool {
	return (r.votedFor == None || r.votedFor == candidateID) &&
		r.log.isUpToDate(lastIndex, lastTerm)
}

func (r *raft) switchToConfig(cfg ClusterConfig, progress progressMap) rt.ConfState {
	r.trk.ClusterConfig = cfg
	r.trk.progress = progress

	r.logger.Infof("[%v] node switched to configuration %+v", r.id, r.trk.ClusterConfig)
	cs := r.trk.ConfState()
	_, ok := r.trk.progress[r.id]
	// step down if leader
	if !ok && r.state == StateLeader {
		r.becomeFollower(r.currentTerm, None)
		return cs
	}

	// the remaining steps require a leader membership
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}
	if r.leaderCommit(r.trk.commitIndex()) {
		r.broadcastAppendEntries(false)
	} else {
		r.broadcastAppendEntries(true)
	}
	return cs
}

func (r *raft) applyConfChange(cc rt.ConfChange) rt.ConfState {
	// config change not apply yet
	cfg, pm, err := func() (ClusterConfig, progressMap, error) {
		c := changer{
			trk:       *r.trk,
			lastIndex: r.log.lastIndex(),
		}
		if leaveJoint(cc) {
			return c.leaveJoint()
		}
		if autoLeave, ok := enterJoint(cc); ok {
			return c.enterJoint(autoLeave, cc.Changes...)
		}
		return c.simple(cc.Changes...)
	}()
	if err != nil {
		panic(err)
	}
	// apply config change
	return r.switchToConfig(cfg, pm)
}

func (r *raft) applySnapshot(snapshot *rt.Snapshot) {
	idx := snapshot.Metadata.LastIndex
	r.log.stableSnapshotTo(idx)
	r.applyTo(idx)
}

func (r *raft) applyTo(idx int64) {
	oldApplyIdx := r.log.lastApplied
	applyIdx := max(idx, oldApplyIdx)
	r.log.applyTo(applyIdx)
	if r.trk.AutoLeave && applyIdx >= r.pendingConfIndex && r.state == StateLeader {
		msg := rt.Message{
			Type: rt.MessageType_Propose,
			Entries: []*rt.Entry{
				{
					Type: rt.EntryType_Config,
					Data: nil,
				},
			},
		}
		if err := r.Step(msg); err != nil {
			r.logger.Debugf("[%v] not initiating automatic transition out of joint configuration %v: %v", r.id, r.trk.ClusterConfig, err)
		} else {
			r.logger.Infof("[%v] initiating automatic transition out of joint configuration %v", r.id, r.trk.ClusterConfig)
		}
	}
}
