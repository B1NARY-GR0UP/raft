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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"strings"
	"sync"
)

type kvStore struct {
	mu       sync.RWMutex
	store    map[string]string
	proposeC chan<- string
}

type kv struct {
	K string
	V string
}

type getSnapshot func() ([]byte, error)

func newKVStore(proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvStore {
	s := &kvStore{
		store:    make(map[string]string),
		proposeC: proposeC,
	}
	// TODO: snapshot
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvStore) Propose(k, v string) {
	var sb strings.Builder
	if err := gob.NewEncoder(&sb).Encode(kv{
		K: k,
		V: v,
	}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- sb.String()
}

func (s *kvStore) Lookup(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.store[k]
	return v, ok
}

func (s *kvStore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// TODO: snapshot
			continue
		}

		for _, data := range commit.data {
			var dataKV kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKV); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.mu.Lock()
			s.store[dataKV.K] = dataKV.V
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvStore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.store)
}
