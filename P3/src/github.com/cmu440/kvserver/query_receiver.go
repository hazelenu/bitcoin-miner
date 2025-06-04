package kvserver

import (
	"errors"

	"github.com/cmu440/actor"
	"github.com/cmu440/kvcommon"
)

// RPC handler implementing the kvcommon.QueryReceiver interface.
// There is one queryReceiver per queryActor, each running on its own port,
// created and registered for RPCs in NewServer.
//
// A queryReceiver MUST answer RPCs by sending a message to its query
// actor and getting a response message from that query actor (via
// ActorSystem's NewChannelRef). It must NOT attempt to answer queries
// using its own state, and it must NOT directly coordinate with other
// queryReceivers - all coordination is done within the actor system
// by its query actor.
type queryReceiver struct {
	// TODO (3A): implement this!
	actorRef *actor.ActorRef
	system   *actor.ActorSystem
}

func NewQueryReceiver(actorRef *actor.ActorRef, system *actor.ActorSystem) *queryReceiver {
	return &queryReceiver{
		actorRef: actorRef,
		system:   system,
	}
}

// Get implements kvcommon.QueryReceiver.Get.
func (rcvr *queryReceiver) Get(args kvcommon.GetArgs, reply *kvcommon.GetReply) error {
	// fmt.Printf("QueryReceiver: Received Get request for key: %s\n", args.Key)
	ref, respChan := rcvr.system.NewChannelRef()
	// fmt.Println("QueryReceiver: Sending message to actor")

	// Send the Get request as a message to the actor
	rcvr.system.Tell(rcvr.actorRef, GetMessage{
		Key:      args.Key,
		RespChan: ref,
	})
	// fmt.Println("QueryReceiver: Waiting for response from actor")

	// Wait for the actor's response
	response := <-respChan
	// fmt.Println("QueryReceiver: Received response from actor")
	getResp, ok := response.(GetResponse)
	if !ok {
		return errors.New("invalid response type for Get")
	}

	// Populate the reply object
	reply.Value = getResp.Value
	reply.Ok = getResp.Ok
	return nil
}

// List implements kvcommon.QueryReceiver.List.
func (rcvr *queryReceiver) List(args kvcommon.ListArgs, reply *kvcommon.ListReply) error {
	// TODO (3A): implement this!
	respRef, respChan := rcvr.system.NewChannelRef()

	// Send the List message to the query actor
	rcvr.system.Tell(rcvr.actorRef, &ListMessage{
		Prefix:   args.Prefix,
		RespChan: respRef,
	})

	// Wait for the response
	response := <-respChan
	listResp, ok := response.(ListResponse)
	if !ok {
		return errors.New("invalid response type for List")
	}

	// Populate the reply object
	reply.Entries = listResp.Entries
	return nil
}

// Put implements kvcommon.QueryReceiver.Put.
func (rcvr *queryReceiver) Put(args kvcommon.PutArgs, reply *kvcommon.PutReply) error {
	// TODO (3A): implement this!
	respRef, _ := rcvr.system.NewChannelRef()

	// Send the Put message to the query actor
	rcvr.system.Tell(rcvr.actorRef, &PutMessage{
		Key:      args.Key,
		Value:    args.Value,
		RespChan: respRef,
	})

	// Wait for the response
	// response := <-respChan
	// _, ok := response.(*PutResponse)
	// if !ok {
	// 	return errors.New("invalid response type for Put")
	// }

	return nil
}
