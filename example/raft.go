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

package main

import (
	"context"
	"log"
	"time"

	"github.com/B1NARY-GR0UP/raft"
	"github.com/B1NARY-GR0UP/raft/raftrpc"
	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
	"github.com/apache/thrift/lib/go/thrift"
)

type raftNode struct {
	proposeC    <-chan string
	confChangeC <-chan rt.ConfChange
	commitC     chan<- *commit
	errorC      chan<- error

	id    int64
	addr  string
	peers map[int64]string
	join  bool

	getSnapshot getSnapshot

	confState     rt.ConfState
	snapshotIndex int64
	appliedIndex  int64

	node      raft.Node
	storage   *raft.MemoryStorage
	transport *raftrpc.Transport

	stopC        chan struct{}
	gatewayStopC chan struct{}
	gatewayDoneC chan struct{}
}

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

func StartRaft(id int64, addr string, peers map[int64]string, join bool,
	getSnapshot getSnapshot, proposeC <-chan string, confChangeC <-chan rt.ConfChange) (<-chan *commit, <-chan error) {
	commitC := make(chan *commit)
	errorC := make(chan error)

	r := &raftNode{
		proposeC:     proposeC,
		confChangeC:  confChangeC,
		commitC:      commitC,
		errorC:       errorC,
		id:           id,
		addr:         addr,
		peers:        peers,
		join:         join,
		getSnapshot:  getSnapshot,
		stopC:        make(chan struct{}),
		gatewayStopC: make(chan struct{}),
		gatewayDoneC: make(chan struct{}),
		storage:      raft.NewMemoryStorage(),
	}
	go r.start()
	return commitC, errorC
}

func (r *raftNode) start() {
	var peers []raft.Peer
	// other nodes
	for id := range r.peers {
		peers = append(peers, raft.Peer{
			ID: id,
		})
	}

	rpcConfig := raftrpc.Config{
		ID:      r.id,
		Addr:    r.addr,
		Handler: raftrpc.HandlerFunc(r.handle),
	}
	r.transport = raftrpc.NewTransport(rpcConfig)
	for id, addr := range r.peers {
		r.transport.AddPeer(id, addr)
	}

	// self
	peers = append(peers, raft.Peer{
		ID: r.id,
	})

	raftConfig := &raft.Config{
		ID:            r.id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       r.storage,
	}
	if r.join {
		r.node = raft.RestartNode(raftConfig)
	} else {
		r.node = raft.StartNode(raftConfig, peers)
	}

	go r.serveRaft()
	go r.serveChannels()
}

func (r *raftNode) serveRaft() {
	if err := r.transport.Start(); err != nil {
		log.Fatal(err)
	}
	select {
	case <-r.gatewayStopC:
	}
	close(r.gatewayStopC)
}

func (r *raftNode) serveChannels() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	go func() {
		for r.proposeC != nil && r.confChangeC != nil {
			select {
			case prop, ok := <-r.proposeC:
				if !ok {
					r.proposeC = nil
				} else {
					if err := r.node.Propose(context.Background(), []byte(prop)); err != nil {
						log.Fatal(err)
					}
				}
			case cc, ok := <-r.confChangeC:
				if !ok {
					r.confChangeC = nil
				} else {
					if err := r.node.ProposeConfChange(context.Background(), cc); err != nil {
						log.Fatal(err)
					}
				}
			}
		}
		close(r.stopC)
	}()

	for {
		select {
		case <-ticker.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			// TODO: saveSnapshot
			// TODO: save persistent state + log entries
			// TODO: apply and publish snapshot
			_ = r.storage.Append(rd.Entries)
			r.transport.Send(r.processMessages(rd.Messages))
			_, ok := r.publishEntries(r.entriesToApply(rd.CommittedEntries))
			if !ok {
				r.stop()
				return
			}
			// TODO: trigger snapshot
			// ready to handle next Ready
			r.node.Advance()
		case <-r.stopC:
			r.stop()
			return
		}
	}
}

func (r *raftNode) handle(ctx context.Context, msg rt.Message) {
	_ = r.node.Step(ctx, msg)
}

func (r *raftNode) processMessages(ms []rt.Message) []rt.Message {
	for _, m := range ms {
		if m.Type == rt.MessageType_InstallSnapshot {
			*m.Snapshot.Metadata.ConfState = r.confState
		}
	}
	return ms
}

func (r *raftNode) entriesToApply(ents []rt.Entry) (nents []rt.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > r.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, r.appliedIndex)
	}
	if r.appliedIndex-firstIdx+1 < int64(len(ents)) {
		nents = ents[r.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (r *raftNode) publishEntries(entries []rt.Entry) (<-chan struct{}, bool) {
	if len(entries) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(entries))
	for _, entry := range entries {
		switch entry.Type {
		case rt.EntryType_Normal:
			if len(entry.Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(entry.Data)
			data = append(data, s)
		case rt.EntryType_Config:
			var cc rt.ConfChange
			deserializer := thrift.NewTDeserializer()
			if err := deserializer.Read(&cc, entry.Data); err != nil {
				log.Fatal(err)
			}
			r.confState = *r.node.ApplyConfChange(cc)
			for _, change := range cc.Changes {
				switch change.Type {
				case rt.ConfChangeType_AddNode:
					// bootstrap config do not have URL, just skip it
					// bootstrap peers already add to transport
					if len(change.NodeURL) > 0 {
						r.transport.AddPeer(change.NodeID, string(change.NodeURL))
					}
				case rt.ConfChangeType_RemoveNode:
					if change.NodeID == r.id {
						log.Println("I've been removed from the cluster! Shutting down.")
						return nil, false
					}
					r.transport.RemovePeer(change.NodeID)
				}
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case r.commitC <- &commit{
			data:       data,
			applyDoneC: applyDoneC,
		}:
		case <-r.stopC:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	r.appliedIndex = entries[len(entries)-1].Index

	return applyDoneC, true
}

func (r *raftNode) stop() {
	r.stopTransportAndGateway()
	close(r.commitC)
	close(r.errorC)
	r.node.Stop()
}

func (r *raftNode) stopTransportAndGateway() {
	r.transport.Stop()
	close(r.gatewayStopC)
	<-r.gatewayDoneC
}
