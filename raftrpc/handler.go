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

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

type Handler interface {
	ServeRPC(ctx context.Context, msg rt.Message)
}

type HandlerFunc func(ctx context.Context, message rt.Message)

func (f HandlerFunc) ServeRPC(ctx context.Context, msg rt.Message) {
	f(ctx, msg)
}
