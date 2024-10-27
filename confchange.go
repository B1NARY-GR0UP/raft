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
	"slices"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

var (
	ErrProgressNotMatch      = errors.New("progress does not match cluster config")
	ErrNoneNilVoters         = errors.New("voters[1] must be nil when not joint")
	ErrTrueAutoLeave         = errors.New("autoLeave must be false when not joint")
	ErrAlreadyJoint          = errors.New("config is already joint")
	ErrZeroVoterJoint        = errors.New("can not make a zero-voter config joint")
	ErrInvalidConfChangeType = errors.New("invalid configure change type")
	ErrApplyIncomingChanges  = errors.New("apply incoming changes failed")
	ErrLeaveNonJointConfig   = errors.New("can not leave a non-joint config")
	ErrApplySimpleToJoint    = errors.New("can not apply simple config change to joint config")
	ErrMoreThanOneChange     = errors.New("more than one voters changed by simple config change")
)

type changer struct {
	trk       tracker
	lastIndex int64
}

func (c changer) leaveJoint() (ClusterConfig, progressMap, error) {
	cfg, pm, err := c.copyAndCheck()
	if err != nil {
		return c.err(err)
	}
	if !joint(cfg) {
		return c.err(ErrLeaveNonJointConfig)
	}
	for id := range outgoing(cfg.Voters) {
		_, isVoter := incoming(cfg.Voters)[id]
		if !isVoter {
			delete(pm, id)
		}
	}
	// clear outgoing
	cfg.Voters[1] = nil
	cfg.AutoLeave = false
	return c.checkAndReturn(cfg, pm)
}

func (c changer) enterJoint(autoLeave bool, ccs ...*rt.ConfChangeSingle) (ClusterConfig, progressMap, error) {
	cfg, p, err := c.copyAndCheck()
	if err != nil {
		return c.err(err)
	}
	if joint(cfg) {
		return c.err(ErrAlreadyJoint)
	}
	if len(incoming(cfg.Voters)) == 0 {
		// adding nodes to an empty config is allowed (e.g. for bootstrap), but enter a joint is not allowed
		return c.err(ErrZeroVoterJoint)
	}
	// lazy init voters[1] (outgoing)
	cfg.Voters[1] = MajorityConfig{}
	// enter joint
	for id := range incoming(cfg.Voters) {
		outgoing(cfg.Voters)[id] = struct{}{}
	}
	if err := c.apply(&cfg, p, ccs...); err != nil {
		return c.err(err)
	}
	cfg.AutoLeave = autoLeave
	return c.checkAndReturn(cfg, p)
}

// simple apply single config changes
func (c changer) simple(ccs ...*rt.ConfChangeSingle) (ClusterConfig, progressMap, error) {
	cfg, pm, err := c.copyAndCheck()
	if err != nil {
		return c.err(err)
	}
	if joint(cfg) {
		return c.err(ErrApplySimpleToJoint)
	}
	if err := c.apply(&cfg, pm, ccs...); err != nil {
		return c.err(err)
	}
	if n := symdiff(incoming(c.trk.Voters), incoming(cfg.Voters)); n > 1 {
		return c.err(ErrMoreThanOneChange)
	}
	return c.checkAndReturn(cfg, pm)
}

// apply new ccs to old cfg
func (c changer) apply(cfg *ClusterConfig, pm progressMap, ccs ...*rt.ConfChangeSingle) error {
	for _, cc := range ccs {
		// if node id is 0, then do not apply this change
		if cc.NodeID == None {
			continue
		}
		switch cc.Type {
		case rt.ConfChangeType_AddNode:
			c.makeVoter(cfg, pm, cc.NodeID)
		case rt.ConfChangeType_RemoveNode:
			c.removeNode(cfg, pm, cc.NodeID)
		default:
			return ErrInvalidConfChangeType
		}
	}
	if len(incoming(cfg.Voters)) == 0 {
		return ErrApplyIncomingChanges
	}
	return nil
}

func (c changer) makeVoter(cfg *ClusterConfig, pm progressMap, id int64) {
	p := pm[id]
	// handle both nil progress and not exist case
	if p == nil {
		// update incoming map in initProgress
		c.initProgress(cfg, pm, id)
		return
	}
	incoming(cfg.Voters)[id] = struct{}{}
}

func (c changer) removeNode(cfg *ClusterConfig, pm progressMap, id int64) {
	if _, ok := pm[id]; !ok {
		return
	}
	delete(incoming(cfg.Voters), id)
	// if the node is still is a voter in outgoing(old) config, then keep the progress
	if _, ok := outgoing(cfg.Voters)[id]; !ok {
		delete(pm, id)
	}
}

func (c changer) initProgress(cfg *ClusterConfig, pm progressMap, id int64) {
	incoming(cfg.Voters)[id] = struct{}{}
	pm[id] = &Progress{
		MatchIndex: 0,
		NextIndex:  c.lastIndex + 1,
	}
}

func (c changer) copyAndCheck() (ClusterConfig, progressMap, error) {
	// copy
	cfg := c.trk.ClusterConfig.clone()
	pm := make(progressMap)
	for k, v := range c.trk.progress {
		// shallow copy
		vp := *v
		pm[k] = &vp
	}
	return c.checkAndReturn(cfg, pm)
}

func (c changer) checkAndReturn(cfg ClusterConfig, pm progressMap) (ClusterConfig, progressMap, error) {
	if err := c.check(cfg, pm); err != nil {
		return ClusterConfig{}, progressMap{}, err
	}
	return cfg, pm, nil
}

func (c changer) check(cfg ClusterConfig, pm progressMap) error {
	for _, id := range cfg.Voters.ids() {
		if _, ok := pm[id]; !ok {
			return ErrProgressNotMatch
		}
	}
	if !joint(cfg) {
		if outgoing(cfg.Voters) != nil {
			return ErrNoneNilVoters
		}
		if cfg.AutoLeave {
			return ErrTrueAutoLeave
		}
	}
	return nil
}

func (c changer) err(err error) (ClusterConfig, progressMap, error) {
	return ClusterConfig{}, nil, err
}

// restore from ConfState
func restore(c changer, cs rt.ConfState) (ClusterConfig, progressMap, error) {
	in, out := toConfChangeSingle(cs)

	var ops []func(changer) (ClusterConfig, progressMap, error)

	if len(out) == 0 {
		for _, cc := range in {
			ops = append(ops, func(c changer) (ClusterConfig, progressMap, error) {
				return c.simple(cc)
			})
		}
	} else {
		for _, cc := range out {
			ops = append(ops, func(c changer) (ClusterConfig, progressMap, error) {
				return c.simple(cc)
			})
		}
		ops = append(ops, func(c changer) (ClusterConfig, progressMap, error) {
			return c.enterJoint(cs.AutoLeave, in...)
		})
	}
	return execute(c, ops...)
}

func execute(c changer, ops ...func(changer) (ClusterConfig, progressMap, error)) (ClusterConfig, progressMap, error) {
	for _, op := range ops {
		cfg, pm, err := op(c)
		if err != nil {
			return c.err(err)
		}
		c.trk.ClusterConfig = cfg
		c.trk.progress = pm
	}
	return c.trk.ClusterConfig, c.trk.progress, nil
}

func toConfChangeSingle(cs rt.ConfState) (in, out []*rt.ConfChangeSingle) {
	for _, id := range cs.VotersOutgoing {
		out = append(out, &rt.ConfChangeSingle{
			Type:   rt.ConfChangeType_AddNode,
			NodeID: id,
		})
	}

	for _, id := range cs.VotersOutgoing {
		in = append(in, &rt.ConfChangeSingle{
			Type:   rt.ConfChangeType_RemoveNode,
			NodeID: id,
		})
	}
	for _, id := range cs.Voters {
		in = append(in, &rt.ConfChangeSingle{
			Type:   rt.ConfChangeType_AddNode,
			NodeID: id,
		})
	}
	return
}

// symdiff calculate the symmetric difference between collection a and b
// e.g. a{1, 2, 3}, b{2, 3, 4}
// symdiff{1, 4}, return 2
func symdiff(a, b map[int64]struct{}) int {
	var n int
	for id := range a {
		if _, ok := b[id]; !ok {
			n++
		}
	}
	for id := range b {
		if _, ok := a[id]; !ok {
			n++
		}
	}
	return n
}

func leaveJoint(cc rt.ConfChange) bool {
	empty := &rt.ConfChange{}
	return cc.Transaction == empty.Transaction && slices.Equal(cc.Changes, empty.Changes)
}

func enterJoint(cc rt.ConfChange) (autoLeave, ok bool) {
	// otherwise use simple config change
	// e.g. type is auto and only has one change
	if cc.Transaction != rt.ConfChangeTransition_Auto || len(cc.Changes) > 1 {
		var al bool
		switch cc.Transaction {
		case rt.ConfChangeTransition_Auto:
			al = true
		case rt.ConfChangeTransition_JointImplicit:
			al = true
		case rt.ConfChangeTransition_JointExplicit:
			al = false
		default:
			// %+v is more detailed than %v (included field name)
			panic(fmt.Sprintf("unknown transition: %+v", cc))
		}
		return al, true
	}
	return false, false
}

// EXAMPLE:
//
// Old Config: voters[0] = {1, 2, 3} voters[1] = {}
// New Config: voters[0] = {2, 3, 4} voters[1] = {}
//
// STEP 1 - Enter Joint Config: voters[0] = {2, 3, 4}, voters[1] = {1, 2, 3}
// STEP 2 - Apply Config, replace node 1 with node 4
// STEP 3 - Leave Joint Config: voters[0] = {2, 3, 4}, voters[1] = {}

func joint(cfg ClusterConfig) bool {
	return len(outgoing(cfg.Voters)) > 0
}

func incoming(voters JointConfig) MajorityConfig {
	return voters[0]
}

func outgoing(voters JointConfig) MajorityConfig {
	return voters[1]
}
