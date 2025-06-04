package kvserver

import (
	"encoding/gob"
	"fmt"
	"github.com/cmu440/actor"
	"math"
	"strings"
	"time"
)

/*

Local/remote sync strategy:

On local actors, each actor maintains a map of local changes to the store - it is updated every time a Put message is processed and updated to the store.
To avoid syncing too frequently, a minimum time interval must pass before local changes are pushed to other local actors.
To ensure the latest update is pushed, heartbeat messages are sent from server to trigger periodic checks on pending changes.
When receiving local Sync messages, if any of the entry is newer than the one in the local store,
update the entry and remove it from local changes if it exists.

On remote actors, only the first actor on each server handles communication with remote actors. Let's call it the leader.
On receiving a list of remote actors from existing server, the leader sends a Connect message to inform them about this new server.
Each leader from remote then send back their entire store for replication through generic Sync message.
When the leader receives Put message that is newer than the one in the store, update the store, local changes and remote changes.
When the leader receives remote Sync messages, if any of the entry is newer than the one in the leader's local store,
update the entry, remove it from remote changes if it exists, AND add this entry to local changes so it gets pushed to other local actors.
When the leader receives local Sync messages, if any of the entry is newer than the one in the leader's local store,
update the entry, remove it from local changes if it exists, AND add this entry to remote changes so it gets pushed to other remote actors.
The periodic sync behavior is the same as in the local setup.

*/

type StoreEntry struct {
	Value string
	Src   string // Uid of the actor that put this entry
	Mtime int64  // Epoch time (ms) when this entry is put
}

// Implement your queryActor in this file.
// See example/counter_actor.go for an example actor using the
// github.com/cmu440/actor package.

type GetMessage struct {
	Key      string
	RespChan *actor.ActorRef
}

type GetResponse struct {
	Value string
	Ok    bool
}

type PutMessage struct {
	Key      string
	Value    string
	RespChan *actor.ActorRef
}

type PutResponse struct{}

type ListMessage struct {
	Prefix   string
	RespChan *actor.ActorRef
}

type ListResponse struct {
	Entries map[string]string
}

// LocalActorRefs: Inform actors of other local actors

type ActorRefsMessage struct {
	LocalActorRefs  []*actor.ActorRef
	RemoteActorRefs []*actor.ActorRef
}

type ActorRefsResponse struct{}

// Heartbeat: Periodically sent from server to fulfill sync maximum latency requirement

type HeartbeatMessage struct{}

type HeartbeatResponse struct{}

// Sync: Send local changes to other actors

type SyncMessage struct {
	IsRemote bool
	Entries  map[string]StoreEntry
}

type SyncResponse struct{}

// Connect: Let remote actors know about this new server

type ConnectMessage struct {
	Who *actor.ActorRef
}

type ConnectResponse struct{}

func init() {
	gob.Register(GetMessage{})
	gob.Register(GetResponse{})
	gob.Register(PutMessage{})
	gob.Register(PutResponse{})
	gob.Register(ListMessage{})
	gob.Register(ListResponse{})
	gob.Register(ActorRefsMessage{})
	gob.Register(ActorRefsResponse{})
	gob.Register(HeartbeatMessage{})
	gob.Register(HeartbeatResponse{})
	gob.Register(SyncMessage{})
	gob.Register(SyncResponse{})
	gob.Register(ConnectMessage{})
	gob.Register(ConnectResponse{})
	gob.Register(StoreEntry{})
}

func (se *StoreEntry) newerThan(other *StoreEntry) bool {
	if se.Mtime != other.Mtime {
		return se.Mtime > other.Mtime
	}
	if se.Src != other.Src {
		return se.Src > other.Src
	}
	return se.Value > other.Value
}

func now() int64 {
	return time.Now().UnixMilli()
}

type queryActor struct {
	context         *actor.ActorContext
	localActorRefs  []*actor.ActorRef
	remoteActorRefs []*actor.ActorRef
	store           map[string]StoreEntry // Local key-value store
	localChanges    map[string]StoreEntry // Entries pending to be sent to other local actors
	remoteChanges   map[string]StoreEntry // Entries pending to be sent to other remote actors
	lastFlushTime   int64
}

// "Constructor" for queryActors, used in ActorSystem.StartActor.
func newQueryActor(context *actor.ActorContext) actor.Actor {
	return &queryActor{
		context:         context,
		localActorRefs:  make([]*actor.ActorRef, 0),
		remoteActorRefs: make([]*actor.ActorRef, 0),
		store:           make(map[string]StoreEntry),
		localChanges:    make(map[string]StoreEntry),
		remoteChanges:   make(map[string]StoreEntry),
		lastFlushTime:   math.MinInt64,
	}
}

// OnMessage implements actor.Actor.OnMessage.
func (actor *queryActor) OnMessage(message any) error {
	switch msg := message.(type) {
	case GetMessage:
		actor.handleGet(msg)
	case PutMessage:
		actor.handlePut(msg)
	case ListMessage:
		actor.handleList(msg)
	case ActorRefsMessage:
		actor.handleActorRefsMessage(msg)
	case HeartbeatMessage:
		actor.handleHeartbeatMessage(msg)
	case SyncMessage:
		actor.handleSyncMessage(msg)
	case ConnectMessage:
		actor.handleConnectMessage(msg)
	default:
		return fmt.Errorf("unknown message type: %T", message)
	}
	return nil
}

func (actor *queryActor) handleGet(msg GetMessage) {
	entry, ok := actor.store[msg.Key]

	response := &GetResponse{
		Value: entry.Value,
		Ok:    ok,
	}
	actor.context.Tell(msg.RespChan, response)
}

// handlePut processes a PutMessage.
func (actor *queryActor) handlePut(msg PutMessage) {
	// fmt.Printf("Actor: Handling Get for key: %s\n", msg.Key)
	newEntry := StoreEntry{
		Value: msg.Value,
		Mtime: now(),
	}
	currentEntry, ok := actor.store[msg.Key]
	if !ok || newEntry.newerThan(&currentEntry) {
		actor.store[msg.Key] = newEntry
		actor.localChanges[msg.Key] = newEntry
		if actor.hasRemote() {
			actor.remoteChanges[msg.Key] = newEntry
		}
		actor.flushChanges()
	}

	response := &PutResponse{}
	actor.context.Tell(msg.RespChan, response)
}

// handleList processes a ListMessage.
func (actor *queryActor) handleList(msg ListMessage) {
	entries := make(map[string]string)
	for key, entry := range actor.store {
		if strings.HasPrefix(key, msg.Prefix) {
			entries[key] = entry.Value
		}
	}

	response := &ListResponse{
		Entries: entries,
	}
	actor.context.Tell(msg.RespChan, response)
}

func (actor *queryActor) hasRemote() bool {
	return len(actor.remoteActorRefs) > 0
}

func (actor *queryActor) flushChanges() {
	if now() < actor.lastFlushTime+minSyncInterval || len(actor.localChanges) == 0 {
		return
	}

	me := actor.context.Self.Uid()

	localMsg := &SyncMessage{IsRemote: false, Entries: actor.localChanges}
	for _, localRef := range actor.localActorRefs {
		if localRef.Uid() == me {
			continue
		}
		actor.context.Tell(localRef, localMsg)
	}
	clear(actor.localChanges)

	if actor.hasRemote() {
		remoteMsg := &SyncMessage{IsRemote: true, Entries: actor.remoteChanges}
		for _, remoteRef := range actor.remoteActorRefs {
			actor.context.Tell(remoteRef, remoteMsg)
		}
	}

	clear(actor.remoteChanges)
	actor.lastFlushTime = now()
}

func (actor *queryActor) handleActorRefsMessage(msg ActorRefsMessage) {
	actor.localActorRefs = msg.LocalActorRefs
	if msg.RemoteActorRefs != nil {
		actor.remoteActorRefs = msg.RemoteActorRefs
		for _, remoteRef := range msg.RemoteActorRefs {
			actor.context.Tell(remoteRef, &ConnectMessage{
				Who: actor.context.Self,
			})
		}
	}
}

func (actor *queryActor) handleHeartbeatMessage(_ HeartbeatMessage) {
	actor.flushChanges()
}

func (actor *queryActor) handleSyncMessage(msg SyncMessage) {
	if msg.IsRemote {
		for key, newEntry := range msg.Entries {
			currentEntry, ok := actor.store[key]
			if !ok || newEntry.newerThan(&currentEntry) {
				actor.store[key] = newEntry
				actor.localChanges[key] = newEntry
				delete(actor.remoteChanges, key)
			}
		}
	} else {
		for key, newEntry := range msg.Entries {
			currentEntry, ok := actor.store[key]
			if !ok || newEntry.newerThan(&currentEntry) {
				actor.store[key] = newEntry
				if actor.hasRemote() {
					actor.remoteChanges[key] = newEntry
				}
				delete(actor.localChanges, key)
			}
		}
	}
	actor.flushChanges()
}

func (actor *queryActor) handleConnectMessage(msg ConnectMessage) {
	actor.remoteActorRefs = append(actor.remoteActorRefs, msg.Who)
	response := &SyncMessage{
		IsRemote: true,
		Entries:  actor.store,
	}
	actor.context.Tell(msg.Who, response)
}
