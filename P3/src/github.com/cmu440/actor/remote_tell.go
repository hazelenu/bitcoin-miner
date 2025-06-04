package actor

import (
	"net/rpc"
)

type RemoteTeller struct {
	system *ActorSystem
}

type RemoteTellArgs struct {
	Ref  *ActorRef
	Mars []byte
}

type RemoteTellReply struct{}

// Calls system.tellFromRemote(ref, mars) on the remote ActorSystem listening
// on ref.Address.
//
// This function should NOT wait for a reply from the remote system before
// returning, to allow sending multiple messages in a row more quickly.
// It should ensure that messages are delivered in-order to the remote system.
// (You may assume that remoteTell is not called multiple times
// concurrently with the same ref.Address).
func remoteTell(client *rpc.Client, ref *ActorRef, mars []byte) {
	copiedRef := *ref
	copiedMars := make([]byte, len(mars))
	copy(copiedMars, mars)
	args := &RemoteTellArgs{Ref: &copiedRef, Mars: copiedMars}
	reply := &RemoteTellReply{}
	_ = client.Go("RemoteTeller.Tell", args, reply, nil)
}

// Registers an RPC handler on server for remoteTell calls to system.
//
// You do not need to start the server's listening on the network;
// just register a handler struct that handles remoteTell RPCs by calling
// system.tellFromRemote(ref, mars).
func registerRemoteTells(system *ActorSystem, server *rpc.Server) error {
	teller := &RemoteTeller{system: system}
	return server.RegisterName("RemoteTeller", teller)
}

func (rt *RemoteTeller) Tell(args *RemoteTellArgs, _ *RemoteTellReply) error {
	rt.system.tellFromRemote(args.Ref, args.Mars)
	return nil
}
