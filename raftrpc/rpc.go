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

package raftrpc

import (
	"context"
	"net"
	"sync"
	"time"

	rr "github.com/B1NARY-GR0UP/raft/raftrpc/kitex_gen/raft"
	"github.com/B1NARY-GR0UP/raft/raftrpc/kitex_gen/raft/rpc"
	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

var _ rr.RPC = (Transporter)(nil)

type Transporter interface {
	rr.RPC
	Start() error
	Send(messages []rt.Message)
	// AddPeer add addr in `host:port` format
	AddPeer(id int64, addr string)
	RemovePeer(id int64)
	Peers() int
	Stop()
}

const (
	_service = "rpc"
	_network = "tcp"
)

var (
	_ rr.RPC      = (*Transport)(nil)
	_ Transporter = (*Transport)(nil)
)

type Transport struct {
	Config
	mu sync.RWMutex
	// k: id, v: client config with addr
	peers map[int64]rpc.Client
}

func NewTransport(config Config) *Transport {
	if err := config.validate(); err != nil {
		panic(err)
	}
	t := &Transport{
		Config: config,
		peers:  make(map[int64]rpc.Client),
	}
	return t
}

func (t *Transport) Do(ctx context.Context, req *rr.Message) (resp *rr.DummyResp, err error) {
	t.Handler.ServeRPC(ctx, RaftMsg(*req))
	resp = &rr.DummyResp{
		Code: rr.Code_Accept,
	}
	return
}

// Start will block
func (t *Transport) Start() error {
	addr, err := net.ResolveTCPAddr(_network, t.Addr)
	if err != nil {
		return err
	}
	srv := rpc.NewServer(t,
		server.WithServiceAddr(addr),
		// might change
		server.WithReadWriteTimeout(time.Second*10),
	)
	if err = srv.Run(); err != nil {
		return err
	}
	return nil
}

func (t *Transport) Send(messages []rt.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(messages) == 0 {
		return
	}
	// validate and convert to rr.Message
	msgs := make([]rr.Message, len(messages))
	for i, msg := range messages {
		if msg.Src != t.ID {
			t.Logger.Error("message src does not consist with node id", "src", msg.Src, "id", t.ID)
			continue
		}
		if msg.Dst == t.ID {
			t.Logger.Error("message dst same with local node id", "dst", msg.Dst, "id", t.ID)
			continue
		}
		msgs[i] = RPCMsg(msg)
	}
	// match dst and send request
	for id, cli := range t.peers {
		for _, msg := range msgs {
			if id == msg.Dst {
				go func() {
					_, err := cli.Do(context.Background(), &msg)
					if err != nil {
						return
					}
				}()
			}
		}
	}
}

func (t *Transport) AddPeer(id int64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// peers with same id will not overwrite the previous one
	if _, ok := t.peers[id]; ok {
		return
	}
	cli, err := rpc.NewClient(_service,
		client.WithHostPorts(addr),
	)
	if err != nil {
		panic(err)
	}
	t.peers[id] = cli
}

func (t *Transport) RemovePeer(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

func (t *Transport) Peers() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.peers)
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.peers)
}
