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
	"fmt"
	"reflect"
	"slices"
	"strings"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

// ClusterConfig is Config (trk.Config)
type ClusterConfig struct {
	Voters    JointConfig
	AutoLeave bool
}

func (c *ClusterConfig) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "voters=%s", c.Voters)
	if c.AutoLeave {
		sb.WriteString(" autoLeave")
	}
	return sb.String()
}

func (c *ClusterConfig) clone() ClusterConfig {
	clone := func(m map[int64]struct{}) map[int64]struct{} {
		if m == nil {
			return nil
		}
		res := make(map[int64]struct{}, len(m))
		for k := range m {
			res[k] = struct{}{}
		}
		return res
	}
	return ClusterConfig{
		Voters: JointConfig{clone(c.Voters[0]), clone(c.Voters[1])},
	}
}

type progressMap map[int64]*Progress

func (p progressMap) clone() map[int64]Progress {
	m := make(map[int64]Progress)
	for k, v := range p {
		m[k] = *v
	}
	return m
}

type tracker struct {
	ClusterConfig
	votes map[int64]bool
	// Volatile state on leaders
	// raft log progress
	progress progressMap
}

func newTracker() *tracker {
	return &tracker{
		ClusterConfig: ClusterConfig{
			Voters: JointConfig{
				MajorityConfig{},
				nil, // populated when used
			},
			AutoLeave: false,
		},
		votes:    make(map[int64]bool),
		progress: make(progressMap),
	}
}

func (t *tracker) voterIDs() []int64 {
	m := t.Voters.IDs()
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// visitProgress visit and apply closure to every progress
func (t *tracker) visitProgress(f func(id int64, p *Progress)) {
	n := len(t.progress)
	// most clusters are smaller than or equal to 7
	var sli [7]int64
	var ids []int64
	// avoid dynamic allocate
	if n > len(sli) {
		ids = make([]int64, n)
	} else {
		ids = sli[:n]
	}
	for id := range t.progress {
		n--
		ids[n] = id
	}
	// insertionSort works well on small data set
	slices.Sort(ids)
	for _, id := range ids {
		f(id, t.progress[id])
	}
}

func (t *tracker) visitVoters(f func(id int64)) {
	// REPLACE:
	// ids := t.voters.ids()
	// insertionSort(ids)
	ids := t.Voters.sortedIDs()
	for _, id := range ids {
		f(id)
	}
}

func (t *tracker) resetVotes() {
	clear(t.votes)
}

func (t *tracker) resetProgress() {
	clear(t.progress)
}

func (t *tracker) recordVote(id int64, voteGranted bool) {
	if _, ok := t.votes[id]; !ok {
		t.votes[id] = voteGranted
	}
}

func (t *tracker) tallyVotes() (granted, rejected int64, _ VoteResult) {
	for _, vote := range t.votes {
		if vote {
			granted++
		} else {
			rejected++
		}
	}
	return granted, rejected, t.Voters.VoteResult(t.votes)
}

// TODO: PLAN B >> raft-go's style
func (t *tracker) commitIndex() int64 {
	return t.Voters.commitIndex(t.progress)
}

func (t *tracker) ConfState() rt.ConfState {
	return rt.ConfState{
		Voters:         incoming(t.Voters).Slice(),
		VotersOutgoing: outgoing(t.Voters).Slice(),
		AutoLeave:      t.AutoLeave,
	}
}

func assertConfStateEqual(a, b rt.ConfState) {
	if IsConfStateEqual(a, b) {
		return
	}
	panic("confstate not equal")
}

func IsConfStateEqual(a, b rt.ConfState) bool {
	s := func(sli *[]int64) {
		// use deep copy avoid effect to original object
		*sli = append([]int64(nil), *sli...)
		slices.Sort(*sli)
	}
	// use pointer to affect a and b, otherwise fields of a, b will not be sorted
	for _, csp := range []*rt.ConfState{&a, &b} {
		s(&csp.Voters)
		s(&csp.VotersOutgoing)
	}
	return reflect.DeepEqual(a, b)
}

type Progress struct {
	NextIndex  int64
	MatchIndex int64
}

func (p *Progress) decreaseNextIndex(v int64) {
	p.NextIndex -= v
}

// update matchIndex and nextIndex
func (p *Progress) update(idx int64) {
	p.MatchIndex = max(p.MatchIndex, idx)
	p.NextIndex = max(p.NextIndex, idx+1)
}
