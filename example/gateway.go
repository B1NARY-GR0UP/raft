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
	"io"
	"log"
	"net/http"
	"strconv"

	rt "github.com/B1NARY-GR0UP/raft/raftthrift"
)

type gateway struct {
	store       *kvStore
	confChangeC chan<- rt.ConfChange
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k := r.RequestURI
	defer r.Body.Close()
	switch r.Method {
	// add kv
	case http.MethodPost:
		v, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		g.store.Propose(k, string(v))
		w.WriteHeader(http.StatusNoContent)
	// get kv
	case http.MethodGet:
		if v, ok := g.store.Lookup(k); ok {
			_, _ = w.Write([]byte(v))
			return
		}
		http.Error(w, "Failed to GET", http.StatusNotFound)
	// add node
	case http.MethodPut:
		url, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		nodeID, err := strconv.ParseInt(k[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := rt.ConfChange{
			Transaction: rt.ConfChangeTransition_Auto,
			Changes: []*rt.ConfChangeSingle{
				{
					Type:    rt.ConfChangeType_AddNode,
					NodeID:  nodeID,
					NodeURL: url,
				},
			},
		}
		g.confChangeC <- cc
		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	// remove node
	case http.MethodDelete:
		nodeID, err := strconv.ParseInt(k[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := rt.ConfChange{
			Transaction: rt.ConfChangeTransition_Auto,
			Changes: []*rt.ConfChangeSingle{
				{
					Type:    rt.ConfChangeType_RemoveNode,
					NodeID:  nodeID,
					NodeURL: nil,
				},
			},
		}
		g.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func StartGateway(store *kvStore, port int, confChangeC chan<- rt.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &gateway{
			store:       store,
			confChangeC: confChangeC,
		},
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// block here
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
