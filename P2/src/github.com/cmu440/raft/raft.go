//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartbeatInterval  = 150
	minElectionTimeout = 600
	maxElectionTimeout = 1200
)

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux               sync.Mutex               // Lock to protect shared access to this peer's state
	peers             []*rpc.ClientEnd         // RPC end points of all peers
	me                int                      // this peer's index into peers[]
	logger            *log.Logger              // We provide you with a separate logger per peer.
	currentTerm       int                      // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int                      // candidateId that received vote in current term (or null if none)
	logs              []logEntry               // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	state             int                      // state of current server (0 = follower, 1 = candidate, 2 = leader)
	commitIndex       int                      // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied       int                      // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex         []int                    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex        []int                    // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyChan         chan ApplyCommand        // channel to send ApplyCommand to the service (or tester)
	voteReplyChan     chan *RequestVoteReply   // channel to send RequestVoteReply back to the main routine
	appendReplyChan   chan *AppendEntriesReply // channel to send AppendEntriesReply back to the main routine
	voteRequestChan   chan eventVoteRequest    // channel to send RequestVote to the service (or tester)
	appendRequestChan chan eventAppendEntries  // channel to send AppendEntries to the service (or tester)
	resetTimerChan    chan struct{}            // channel to reset timer
}

type logEntry struct {
	Term    int // term when entry was logged
	Command interface{}
}

type eventVoteRequest struct {
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	signal chan struct{}
}

type eventAppendEntries struct {
	args   *AppendEntriesArgs
	reply  *AppendEntriesReply
	signal chan struct{}
}

// GetState
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isLeader bool
	// TODO - Your code here (2A)
	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	rf.mux.Unlock()
	return me, term, isLeader
}

// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// # Please note: Field names must start with capital letters!
type RequestVoteReply struct {
	// TODO - Your data here (2A)
	Term int
	Vote bool
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // success loged or not
	Id      int  // peer id
	Index   int  // last log index
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		peers:             peers,
		me:                me,
		currentTerm:       0,
		votedFor:          -1,
		state:             Follower,
		applyChan:         applyCh,
		logs:              make([]logEntry, 1),
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		voteRequestChan:   make(chan eventVoteRequest),
		voteReplyChan:     make(chan *RequestVoteReply),
		appendRequestChan: make(chan eventAppendEntries),
		appendReplyChan:   make(chan *AppendEntriesReply),
		resetTimerChan:    make(chan struct{}),
	}

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	rf.logs[0] = logEntry{
		Command: nil,
		Term:    -1,
	}

	go rf.MainRoutine()

	return rf
}

// MainRoutine
// ===========
//
// MainRoutine is the main routine for the Raft server
// It handles all the logic for the Raft server
// It will handle the following:
// 1. Election timeout
// 2. Heartbeat
// 3. RequestVote
// 4. AppendEntries
// 5. ApplyCommand
// 6. ResetTimer
func (rf *Raft) MainRoutine() {
	electionTimer := randomTimer()
	heartBeatTimer := time.NewTimer(time.Millisecond * time.Duration(HeartbeatInterval))
	heartBeatTimer.Stop()
	votes := 1
	for {
		select {
		case <-rf.resetTimerChan:
			heartBeatTimer.Stop()
			electionTimer = randomTimer()

		case <-electionTimer.C:
			votes = 1
			electionTimer = randomTimer()

			rf.mux.Lock()
			me := rf.me
			rf.logger.Printf("%v starts election\n", rf.me)
			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = me
			requestVoteArgs := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			rf.mux.Unlock()
			for peer := range rf.peers {
				if peer != me {
					go rf.sendRequestVote(peer, requestVoteArgs, &RequestVoteReply{})
				}
			}

		case reply := <-rf.appendReplyChan:
			rf.mux.Lock()
			majority := len(rf.peers) / 2
			if reply.Success {
				position := reply.Index
				rf.matchIndex[reply.Id] = position
				rf.nextIndex[reply.Id] = position + 1

				currentCommit := rf.commitIndex
				for index := currentCommit + 1; index <= position; index++ {
					agreementCount := 0
					for _, matched := range rf.matchIndex {
						if matched >= index {
							agreementCount++
						}
						if agreementCount >= majority {
							if rf.logs[index].Term == rf.currentTerm {
								rf.commitIndex = index
							}
							break
						}
					}
				}

				for appliedIndex := rf.lastApplied + 1; appliedIndex <= rf.commitIndex; appliedIndex++ {
					commandToApply := ApplyCommand{
						Index:   appliedIndex,
						Command: rf.logs[appliedIndex].Command,
					}
					rf.applyChan <- commandToApply
					rf.lastApplied = appliedIndex
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				heartBeatTimer.Stop()
				electionTimer = randomTimer()
			} else {
				rf.nextIndex[reply.Id]--
			}
			rf.mux.Unlock()

		case reply := <-rf.voteReplyChan:
			rf.mux.Lock()
			majority := len(rf.peers) / 2
			me := rf.me
			if rf.state != Candidate {
			}
			if reply.Vote {
				votes++
				if votes >= majority {
					heartBeatTimer.Reset(time.Millisecond * time.Duration(0))
					electionTimer.Stop()
					rf.state = Leader
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[me] = len(rf.logs) - 1
				}
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				electionTimer = randomTimer()
			}
			rf.mux.Unlock()

		case <-heartBeatTimer.C:
			heartBeatTimer.Reset(time.Millisecond * time.Duration(HeartbeatInterval))
			rf.mux.Lock()
			me := rf.me
			if rf.state != Leader {
				rf.mux.Unlock()
				break
			}
			peers := rf.peers

			rf.mux.Unlock()

			for peer := range peers {
				if peer == me {
					continue
				}
				rf.mux.Lock()
				var entries []logEntry = nil
				lastLogIndex := len(rf.logs) - 1
				if lastLogIndex >= rf.nextIndex[peer] {
					entries = rf.logs[rf.nextIndex[peer]:]
				}
				heartbeat := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{
					Id:    peer,
					Index: lastLogIndex,
				}
				rf.mux.Unlock()
				go rf.sendAppendEntries(peer, &heartbeat, &reply)
			}
		}
	}
}

func randomTimer() *time.Timer {
	return time.NewTimer(time.Duration(rand.Intn(maxElectionTimeout-minElectionTimeout)+minElectionTimeout) * time.Millisecond)
}

// RequestVote
// ===========
//
// Request Vote RPC handler
// This function is called when a server receives a RequestVote RPC
// It will handle the following:
// 1. Update term and become a follower if needed
// 2. Handle outdated term
// 3. Handle vote request
// 4. Reset timer
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()
	vote := false
	reset := false

	rf.logger.Printf("%v with term %v received vote request from %v with term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reset = true
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Vote = vote
		rf.mux.Unlock()
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.logs) - 1
		reset = true

		if args.LastLogTerm > rf.logs[lastLogIndex].Term {
			vote = true
			rf.votedFor = args.CandidateId
		} else if args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex {
			vote = true
			rf.votedFor = args.CandidateId
		}
	}

	reply.Term = rf.currentTerm
	reply.Vote = vote
	rf.logger.Printf("%v votes %v with term %v, current term for self is %v\n", rf.me, reply, args.Term, rf.currentTerm)

	if reset {
		rf.resetTimerChan <- struct{}{}
	}
	rf.mux.Unlock()
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.logger.Printf("%d send request to %d, getting reply %v", rf.me, server, reply)
		rf.voteReplyChan <- reply
	} else {
		rf.logger.Printf("%d failed to send or getting back from %d", rf.me, server)
	}
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()

	term := rf.currentTerm
	isLeader := rf.state == Leader
	idx := len(rf.logs)
	me := rf.me

	if isLeader {
		rf.matchIndex[me] = idx
		rf.nextIndex[me] = idx + 1
		rf.logs = append(rf.logs, logEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		idx = len(rf.logs)
		rf.logger.Printf("%v with term %v put command %v with index %v", me, rf.currentTerm, command, idx-1)
	}

	rf.mux.Unlock()
	return idx - 1, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// TODO - Your code here, if desired
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.appendReplyChan <- reply
	}
	return ok
}

// AppendEntries
// =============
//
// AppendEntries RPC handler
// This function is called when a server receives an AppendEntries RPC
// It will handle the following:
// 1. Update term and become a follower if needed
// 2. Handle outdated term
// 3. Handle log mismatch
// 4. Handle heartbeat
// 5. Update commit index
// 6. Apply committed entries
// 7. Reset timer
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	logMismatch := false
	appendSuccess := false

	// Update term and become a follower if needed
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Handle outdated term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mux.Unlock()
		return
	}

	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		logMismatch = true
		if args.PrevLogIndex < len(rf.logs) {
			rf.logs = rf.logs[:args.PrevLogIndex]
		}
	}

	if args.Entries == nil && !logMismatch {
		rf.logger.Printf("%d heartbeat from Leader %d at term %d", rf.me, args.LeaderId, args.Term)
		appendSuccess = true
	} else if !logMismatch {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		appendSuccess = true
	}

	reply.Term = rf.currentTerm
	reply.Success = appendSuccess && !logMismatch

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		var newCommitIndex int
		if args.LeaderCommit < len(rf.logs)-1 {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = len(rf.logs) - 1
		}
		rf.logger.Printf("%d updating commit index from %d to %d", rf.me, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
	}

	// Apply committed entries
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyCmd := ApplyCommand{
			Index:   rf.lastApplied,
			Command: rf.logs[rf.lastApplied].Command,
		}
		rf.logger.Printf("%v applied log index %v: %v", rf.me, applyCmd.Index, applyCmd.Command)
		rf.applyChan <- applyCmd
	}

	if reply.Success || logMismatch {
		rf.resetTimerChan <- struct{}{}
	}

	rf.mux.Unlock()
}
