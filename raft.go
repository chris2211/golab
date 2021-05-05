package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)

import "6.824-golabs/src/labrpc"

// import "bytes"
// import "encoding/gob"




// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	isLeader		bool
	resetTimer		chan struct{}
	electionTimer	*time.Timer
	electionTimeout		time.Duration
	hearBeatInterval	time.Duration
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	CurrentTerm		int
	VotedFor		int
	Logs			[]LogEntry
	commitCond		*sync.Cond
	newEntryCond	[]*sync.Cond
	commitIndex		int
	lastApplied		int


	nextIndex		[]int

	matchIndex		[]int

	applyCh			chan ApplyMsg
	shutdown		chan struct{}
}


func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.isLeader
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0
	isLeader := false
	select {
	case <-rf.shutdown:
		return index, term, isLeader
	default:

	}
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		log := LogEntry{rf.CurrentTerm, command}
		rf.Logs = append(rf.Logs, log)
		index = len(rf.Logs) - 1
		term = rf.CurrentTerm
		isLeader = true

		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		rf.wakeupConsistencyCheck()

		rf.persist()
	}
	return index, term, isLeader
}


// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.isLeader = false
	rf.VotedFor = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{
		Term:0,
		Command:nil,
	}
	rf.shutdown = make(chan struct{})
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.Logs)
	}
	rf.resetTimer = make(chan struct{})
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.newEntryCond = make([]*sync.Cond, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.newEntryCond[i] = sync.NewCond(&rf.mu)
	}
	rf.electionTimeout = time.Millisecond * (400 +
		time.Duration(rand.Int63() % 400))
	rf.electionTimer = time.NewTimer(rf.electionTimeout)

	rf.hearBeatInterval = time.Millisecond * 200
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.electionDaemon()

	go rf.applyEntryDaemon()
	return rf
}
