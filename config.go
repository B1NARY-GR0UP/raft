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

import "errors"

var (
	ErrNoneID        = errors.New("cannot use none as id")
	ErrHeartbeatTick = errors.New("heartbeat tick must be greater than 0")
	ErrElectionTick  = errors.New("election tick must be greater than heartbeat tick")
	ErrNilStorage    = errors.New("storage cannot be nil")
)

// Config is the struct used to config the raft library
//
// NOTE: Config is not represent the cluster config in the raft concept
type Config struct {
	ID int64

	// broadcastTime << electionTimeout << MTBF (Mean Time Between Failures)
	//
	// - broadcastTime: Average time it takes a server to send RPCs in parallel to every server in the cluster and receive their responses
	// - MTBF: average time between failures for a single server
	//
	// Typically broadcastTime range from 0.5ms to 20ms, depending on storage technology
	// electionTimeout range from 10ms to 500ms
	//
	// electionTimeout occurs when number of calling Node.Tick but without receiving any RPC from current leader node
	ElectionTick int
	// number of calling Node.Tick needed to represent one Raft heartbeat
	HeartbeatTick int

	// Storage is an interface for the storage of Raft log
	//
	// Persistent state on all servers:
	// - currentTerm
	// - voteFor
	// - log[]
	//
	// The implementation can be non-persistent when using with WAL
	//
	// The implementation of etcd use a memory storage for Storage but with the use of WAL,
	// they can ensure that the Raft log be stored persistently in WAL and can recover after reboot
	// even if the Storage is base on memory
	Storage Storage
	// LastApplied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// LastApplied. If LastApplied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	LastApplied int64
	Logger      Logger
}

// validate Raft config
func (c *Config) validate() error {
	if c.ID == None {
		return ErrNoneID
	}
	if c.HeartbeatTick <= 0 {
		return ErrHeartbeatTick
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return ErrElectionTick
	}
	if c.Storage == nil {
		return ErrNilStorage
	}
	if c.Logger == nil {
		c.Logger = getLogger()
	}
	return nil
}
