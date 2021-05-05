package raft

import (
	"sync"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	currentTerm        int
	VoteGranted 	   bool
}


 // example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	select {
	case <- rf.shutdown:
		return
	default:
	}


	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIdx := len(rf.Logs) - 1
	lastLogTerm := rf. Logs[lastLogIdx].Term

	if args.Term < rf.CurrentTerm {
		reply.currentTerm = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.isLeader = false
			rf.VotedFor = -1
		}

		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx ||
				args.LastLogTerm > lastLogTerm {
				rf.resetTimer <- struct{}{}
				rf.isLeader = false
				rf.VotedFor = args.CandidateId
				reply.VoteGranted = true

			}
		}

		rf.persist()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout)

		case <-rf.electionTimer.C:
			go rf.canvassVotes()

			rf.electionTimer.Reset(rf.electionTimeout)
		}

	}
}

func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer  rf.mu.Unlock()

	rf.CurrentTerm += 1
	rf.VotedFor = rf.me

	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.Logs) - 1
	args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
}


func (rf *Raft) canvassVotes() {
	select {
	case <-rf.shutdown:
		return
	default:
	}

	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)

	peers := len(rf.peers)

	replyCh := make(chan RequestVoteReply, peers)
	var wg sync.WaitGroup

	for i := 0;i < peers;i++ {
		if i == rf.me {
			rf.resetTimer <- struct{}{}
		} else {
			wg.Add(1)

			go func(n int) {
				defer wg.Done()
				var reply RequestVoteReply

				doneCh := make(chan bool, 1)
				go func() {
					ok := rf.sendRequestVote(n,&voteArgs,&reply)
					doneCh <- ok
				}()

				select {
				case ok := <-doneCh:
					if !ok {
						return
					}

					replyCh <- reply
				}
			}(i)

			rf.sendRequestVote(0, &voteArgs,nil)
		}
	}

	go func() {wg.Wait(); close(replyCh)}()
	var votes = 1

	for reply := range replyCh {
		if reply.VoteGranted == true {
			votes++
			if votes > peers/2 {
				rf.mu.Lock()
				rf.isLeader = true
				rf.mu.Unlock()

				rf.resetOnElection()
				go rf.heartbeatDaemon()
				go rf.logEntryAgreeDaemon()
				return
			}
		} else if reply.currentTerm > voteArgs.Term {
			rf.mu.Lock()
			rf.isLeader = false
			rf.VotedFor = -1
			rf.CurrentTerm = reply.currentTerm
			rf.persist()
			rf.mu.Unlock()
			rf.resetTimer <- struct{}{}
			return
		}
	}
}

func (rf *Raft) heartbeatDaemon() {
	for {
		if _,isleader := rf.GetState();isleader {
			rf.resetTimer <- struct{}{}
		} else {
			break
		}

		time.Sleep(rf.hearBeatInterval)
	}
}

func (rf *Raft) resetOnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	peerCount := len(rf.peers)
	logLength := len(rf.Logs)

	for i := 0;i < peerCount; i++ {
		rf.nextIndex[i] = logLength
		rf.matchIndex[i] = 0

		if i == rf.me {
			rf.matchIndex[i] = logLength - 1
		}
	}
}




