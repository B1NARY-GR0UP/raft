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
	"crypto/rand"
	"math/big"
	"sync"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
	"github.com/apache/thrift/lib/go/thrift"
)

func IsEmptySnapshot(snapshot rt.Snapshot) bool {
	return snapshot.Metadata == nil || snapshot.Metadata.LastIndex == 0
}

var clientMessage = map[rt.MessageType]struct{}{
	rt.MessageType_Propose: {},
}

func isClientMessage(t rt.MessageType) bool {
	_, ok := clientMessage[t]
	return ok
}

// internal messages are different from local messages
// internal messages should only be used inside node itself
// local messages are messages which dst is node itself
var internalMessage = map[rt.MessageType]struct{}{
	rt.MessageType_Campaign:      {},
	rt.MessageType_Beat:          {},
	rt.MessageType_StorageAppend: {},
	rt.MessageType_StorageApply:  {},
}

func isInternalMessage(t rt.MessageType) bool {
	_, ok := internalMessage[t]
	return ok
}

var rpcReqMessage = map[rt.MessageType]struct{}{
	rt.MessageType_AppendEntries:   {},
	rt.MessageType_RequestVote:     {},
	rt.MessageType_InstallSnapshot: {},
}

func isRPCReqMessage(t rt.MessageType) bool {
	_, ok := rpcReqMessage[t]
	return ok
}

var rpcRespMessage = map[rt.MessageType]struct{}{
	rt.MessageType_AppendEntriesResp:   {},
	rt.MessageType_RequestVoteResp:     {},
	rt.MessageType_InstallSnapshotResp: {},
}

func isRPCRespMessage(t rt.MessageType) bool {
	_, ok := rpcRespMessage[t]
	return ok
}

var globalRand = &syncRand{}

type syncRand struct {
	mu sync.Mutex
}

// Intn return a random value in [0, n)
func (r *syncRand) Intn(n int) int {
	r.mu.Lock()
	// higher security and wider range compare to rand.Intn
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	r.mu.Unlock()
	return int(v.Int64())
}

func EntryValues(entries []*rt.Entry) []rt.Entry {
	res := make([]rt.Entry, len(entries))
	for i, entry := range entries {
		if entry != nil {
			res[i] = *entry
		}
	}
	return res
}

func EntryPointers(entries []rt.Entry) []*rt.Entry {
	res := make([]*rt.Entry, len(entries))
	for i, entry := range entries {
		res[i] = &entry
	}
	return res
}

func TMarshal(ctx context.Context, data thrift.TStruct) ([]byte, error) {
	serializer := thrift.NewTSerializer()
	return serializer.Write(ctx, data)
}

func TUnmarshal(data []byte, v thrift.TStruct) error {
	deserializer := thrift.NewTDeserializer()
	return deserializer.Read(v, data)
}
