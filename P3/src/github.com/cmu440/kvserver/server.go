// Package kvserver implements the backend server for a
// geographically distributed, highly available, NoSQL key-value store.
package kvserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/cmu440/actor"
)

// A single server in the key-value store, running some number of
// query actors - nominally one per CPU core. Each query actor
// provides a key/value storage service on its own port.
//
// Different query actors (both within this server and across connected
// servers) periodically sync updates (Puts) following an eventually
// consistent, last-writer-wins strategy.
type Server struct {
	actorSystem    *actor.ActorSystem // Actor system managing query actors
	queryActors    []*actor.ActorRef  // References to the query actors
	rpcListeners   []net.Listener     // Listeners for RPC servers
	closeOnce      sync.Once          // Ensures Close is idempotent
	closeHeartbeat context.CancelFunc
}

// OPTIONAL: Error handler for ActorSystem.OnError.
//
// Print the error or call debug.PrintStack() in this function.
// When starting an ActorSystem, call ActorSystem.OnError(errorHandler).
// This can help debug server-side errors more easily.
func errorHandler(err error) {
}

// Starts a server running queryActorCount query actors.
//
// The server's actor system listens for remote messages (from other actor
// systems) on startPort. The server listens for RPCs from kvclient.Clients
// on ports [startPort + 1, startPort + 2, ..., startPort + queryActorCount].
// Each of these "query RPC servers" answers queries by asking a specific
// query actor.
//
// remoteDescs contains a "description" string for each existing server in the
// key-value store. Specifically, each slice entry is the desc returned by
// an existing server's own NewServer call. The description strings are opaque
// to callers, but typically an implementation uses JSON-encoded data containing,
// e.g., actor.ActorRef's that remote servers' actors should contact.
//
// Before returning, NewServer starts the ActorSystem, all query actors, and
// all query RPC servers. If there is an error starting anything, that error is
// returned instead.
func NewServer(startPort int, queryActorCount int, remoteDescs []string) (server *Server, desc string, err error) {

	// Tips:
	// - The "HTTP service" example in the net/rpc docs does not support
	// multiple RPC servers in the same process. Instead, use the following
	// template to start RPC servers (adapted from
	// https://groups.google.com/g/Golang-Nuts/c/JTn3LV_bd5M/m/cMO_DLyHPeUJ ):
	//
	//  rpcServer := rpc.NewServer()
	//  err := rpcServer.RegisterName("QueryReceiver", [*queryReceiver instance])
	//  ln, err := net.Listen("tcp", ...)
	//  go func() {
	//    for {
	//      conn, err := ln.Accept()
	//      if err != nil {
	//        return
	//      }
	//      go rpcServer.ServeConn(conn)
	//    }
	//  }()
	//
	// - To start query actors, call your ActorSystem's
	// StartActor(newQueryActor), where newQueryActor is defined in ./query_actor.go.
	// Do this queryActorCount times. (For the checkpoint tests,
	// queryActorCount will always be 1.)
	// - remoteDescs and desc: see doc comment above.
	// For the checkpoint, it is okay to ignore remoteDescs and return "" for desc.

	actorSystem, err := actor.NewActorSystem(startPort)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start actor system: %v", err)
	}
	actorSystem.OnError(errorHandler)

	heartbeatCtx, closeHeartbeat := context.WithCancel(context.Background())
	server = &Server{
		actorSystem:    actorSystem,
		queryActors:    make([]*actor.ActorRef, queryActorCount),
		rpcListeners:   make([]net.Listener, queryActorCount),
		closeHeartbeat: closeHeartbeat,
	}

	// Start query actors
	for i := 0; i < queryActorCount; i++ {
		server.queryActors[i] = actorSystem.StartActor(newQueryActor)
	}

	remoteActors := make([]*actor.ActorRef, 0)
	// Unmarshal remote actors from desc
	for _, remoteDesc := range remoteDescs {
		ref := &actor.ActorRef{}
		err := json.Unmarshal([]byte(remoteDesc), ref)
		if err != nil {
			return nil, "", err
		}
		remoteActors = append(remoteActors, ref)
	}

	for i, actorRef := range server.queryActors {
		if i == 0 {
			server.actorSystem.Tell(actorRef, ActorRefsMessage{LocalActorRefs: server.queryActors, RemoteActorRefs: remoteActors})
		} else {
			server.actorSystem.Tell(actorRef, ActorRefsMessage{LocalActorRefs: server.queryActors})
		}
	}

	go func() {
		timer := time.NewTicker(heartbeatInterval * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-timer.C:
				for _, actorRef := range server.queryActors {
					server.actorSystem.Tell(actorRef, HeartbeatMessage{})
				}
			}
		}
	}()

	// Start RPC servers for each query actor
	for i := 0; i < queryActorCount; i++ {
		port := startPort + 1 + i
		rpcServer := rpc.NewServer()
		queryReceiver := NewQueryReceiver(server.queryActors[i], actorSystem)

		if err := rpcServer.RegisterName("QueryReceiver", queryReceiver); err != nil {
			server.Close() // Cleanup on error
			return nil, "", fmt.Errorf("failed to register RPC server on port %d: %v", port, err)
		}

		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			server.Close() // Cleanup on error
			return nil, "", fmt.Errorf("failed to listen on port %d: %v", port, err)
		}
		server.rpcListeners[i] = ln

		// Start serving RPCs on this listener
		go func(listener net.Listener) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return // Listener closed, exit goroutine
				}
				go rpcServer.ServeConn(conn)
			}
		}(ln)
	}

	// Generate server description for remote syncing
	descStruct := server.queryActors[0]
	descBytes, err := json.Marshal(descStruct)
	if err != nil {
		server.Close() // Cleanup on error
		return nil, "", fmt.Errorf("failed to generate server description: %v", err)
	}
	desc = string(descBytes)

	return server, desc, nil
}

// OPTIONAL: Closes the server, including its actor system
// and all RPC servers.
//
// You are not required to implement this function for full credit; the tests end
// by calling Close but do not check that it does anything. However, you
// may find it useful to implement this so that you can run multiple/repeated
// tests in the same "go test" command without cross-test interference (in
// particular, old test servers' squatting on ports.)
//
// Likewise, you may find it useful to close a partially-started server's
// resources if there is an error in NewServer.
func (server *Server) Close() {
	server.closeOnce.Do(func() {
		// Close all RPC listeners
		for _, ln := range server.rpcListeners {
			if ln != nil {
				ln.Close()
			}
		}
		if server.actorSystem != nil {
			server.actorSystem.Close()
		}
		server.closeHeartbeat()
	})
}
