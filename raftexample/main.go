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
	"flag"
	"log"
	"strconv"
	"strings"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

func main() {
	nodeID := flag.String("id", "must specify", "node id")
	nodeAddr := flag.String("addr", "must specify", "node addr")
	peerIDs := flag.String("peerids", "must specify", "peers id")
	peerAddrs := flag.String("peeraddrs", "must specify", "peers addr")

	gatewayPort := flag.Int("port", 9121, "gateway port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	id, err := strconv.ParseInt(*nodeID, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	addr := *nodeAddr
	var ids []int64
	for _, peerid := range strings.Split(*peerIDs, ",") {
		pid, _ := strconv.ParseInt(peerid, 10, 64)
		ids = append(ids, pid)
	}
	addrs := strings.Split(*peerAddrs, ",")
	// len(ids) == len(addrs)
	peers := make(map[int64]string, len(ids))
	for i := range len(addrs) {
		peers[ids[i]] = addrs[i]
	}

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan rt.ConfChange)
	defer close(confChangeC)

	var kvs *kvStore
	commitC, errorC := StartRaft(id, addr, peers, *join, func() ([]byte, error) { return kvs.getSnapshot() }, proposeC, confChangeC)
	store := newKVStore(proposeC, commitC, errorC)
	StartGateway(store, *gatewayPort, confChangeC, errorC)
}
