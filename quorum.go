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
	"maps"
	"slices"
	"strings"
)

type VoteResult uint32

const (
	_ = iota
	VotePending
	VoteLost
	VoteWon
)

type MajorityConfig map[int64]struct{}

func (mc MajorityConfig) String() string {
	ids := make([]int64, 0, len(mc))
	for id := range mc {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var sb strings.Builder
	sb.WriteByte('(')
	for i := range ids {
		if i > 0 {
			sb.WriteByte(' ')
		}
		_, _ = fmt.Fprint(&sb, ids[i])
	}
	sb.WriteByte(')')
	return sb.String()
}

func (mc MajorityConfig) Slice() []int64 {
	var sli []int64
	for id := range mc {
		sli = append(sli, id)
	}
	slices.Sort(sli)
	return sli
}

func (mc MajorityConfig) commitIndex(progress progressMap) int64 {
	n := len(mc)
	if n == 0 {
		return Infinite
	}

	var sli [7]int64
	// match index slice
	var mis []int64
	if n > len(sli) {
		mis = make([]int64, n)
	} else {
		mis = sli[:n]
	}

	i := n - 1
	for id := range mc {
		if lp, ok := progress[id]; ok {
			mis[i] = lp.MatchIndex
			i--
		}
	}

	slices.Sort(mis)

	idx := n - (n/2 + 1)
	return mis[idx]
}

func (mc MajorityConfig) VoteResult(votes map[int64]bool) VoteResult {
	if len(mc) == 0 {
		return VoteWon
	}

	var accept int
	var reject int
	for id := range mc {
		granted, ok := votes[id]
		if !ok {
			continue
		}
		if granted {
			accept++
		} else {
			reject++
		}
	}

	quorum := len(mc)/2 + 1
	if accept >= quorum {
		return VoteWon
	}
	if reject >= quorum {
		return VoteLost
	}
	return VotePending
}

type JointConfig [2]MajorityConfig

func (jc JointConfig) IDs() map[int64]struct{} {
	m := make(map[int64]struct{})
	for _, mc := range jc {
		for id := range mc {
			m[id] = struct{}{}
		}
	}
	return m
}

func (jc JointConfig) String() string {
	if len(jc[1]) > 0 {
		return jc[0].String() + "&&" + jc[1].String()
	}
	return jc[0].String()
}

func (jc JointConfig) ids() []int64 {
	m := make(map[int64]struct{})
	for _, mc := range jc {
		for id := range mc {
			m[id] = struct{}{}
		}
	}
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

// use go 1.23 new feature
func (jc JointConfig) sortedIDs() []int64 {
	m := make(map[int64]struct{})
	for _, mc := range jc {
		for id := range mc {
			m[id] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(m))
}

func (jc JointConfig) commitIndex(progress progressMap) int64 {
	i1 := jc[0].commitIndex(progress)
	i2 := jc[1].commitIndex(progress)
	if i1 < i2 {
		return i1
	}
	return i2
}

func (jc JointConfig) VoteResult(votes map[int64]bool) VoteResult {
	r1 := jc[0].VoteResult(votes)
	r2 := jc[1].VoteResult(votes)

	if r1 == r2 {
		return r1
	}
	if r1 == VoteLost || r2 == VoteLost {
		return VoteLost
	}
	return VotePending
}
