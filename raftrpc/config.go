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
	"errors"

	"github.com/B1NARY-GR0UP/raft"
)

var (
	ErrNoneID     = errors.New("id must not be none")
	ErrEmptyAddr  = errors.New("addr must not be empty")
	ErrNilHandler = errors.New("handler must not be nil")
)

type Config struct {
	// ID must be consistent with raft node id
	ID int64
	// Addr in `host:port` format
	Addr    string
	Handler Handler
	Logger  raft.Logger
}

func (c *Config) validate() error {
	if c.ID == raft.None {
		return ErrNoneID
	}
	if c.Addr == "" {
		return ErrEmptyAddr
	}
	if c.Handler == nil {
		return ErrNilHandler
	}
	if c.Logger == nil {
		c.Logger = raft.SLog
	}
	return nil
}
