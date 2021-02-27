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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
//import "../labrpc"
import "lab/labrpc"

// import "bytes"
// import "../labgob"

const ElectionTimeoutBase = 250                    // 250ms
const HeartBeatTimeout = 150 * time.Millisecond    // 150ms
const CheckTimeoutDuration = 10 * time.Millisecond // 10ms
const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command string // todo
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	logs        []LogEntry // todo
	commitIndex int
	lastApplied int

	peerKind    int
	lastUpdated time.Time
	leaderId    int //todo waiting for init
	votesGained int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.peerKind == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId   int
	CandidateTerm int
	LastLogIndex  int
	LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// Let a leader or candidate downgrade to a follower.
// Fresh its state if it's already a follower
//
func (rf *Raft) TurnFollower() {
	rf.mu.Lock()

	rf.peerKind = Follower
	rf.votedFor = -1
	rf.lastUpdated = time.Now()

	rf.mu.Unlock()
}

//
// get lastLogIndex and lastLogTerm
//
func (rf *Raft) GetLastLogInfo() (index int, term int) {
	length := len(rf.logs)
	if length == 0 {
		return 0, 0
	}
	return length - 1, rf.logs[length-1].Term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if rf.HasVotedForOthers(args.CandidateId) {
		DPrintf("Server%v(state: %v) vote for others(id: %v),candidate:%v，term:%v", rf.me, rf.peerKind, rf.votedFor, args.CandidateId,args.CandidateTerm)
	}
	if rf.currentTerm > args.CandidateTerm || rf.HasVotedForOthers(args.CandidateId) {
		reply.VoteGranted = false
		return
	}
	rf.lastUpdated = time.Now() // todo important
	if rf.ArgsMoreUpdate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("Server %v votes for Candidate %v, state: %v", rf.me, args.CandidateId, rf.peerKind)
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
	return
	// todo
}

func (rf *Raft) HasVotedForOthers(id int) bool {
	return rf.votedFor != -1 && rf.votedFor != id
}

//
//whether the candidate’s log is at least as up-to-date as this peer
//
func (rf *Raft) ArgsMoreUpdate(LastLogIndex int, LastLogTerm int) bool {
	logsLength := len(rf.logs)
	if logsLength == 0 {
		return true
	} else {
		logIndex := logsLength - 1
		logTerm := rf.logs[logIndex].Term
		if LastLogTerm > logTerm || (LastLogTerm == logTerm && LastLogIndex >= logIndex) {
			return true
		}
	}
	return false
}

//
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
//

//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

func (rf *Raft) sendRequestVote(server int) {
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.GetLastLogInfo()
	args := RequestVoteArgs{rf.me, term, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}
	if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		//DPrintf("Candidate %v fail to send RequestVote rpc to server %v, term: %v\n", rf.me, server, term)
		return
	}
	if reply.Term > term && rf.peerKind == Candidate {
		DPrintf("Candidate %v downgrade when send RV rpc to server %v, currentTerm %v\n", rf.me, server, term)
		rf.TurnFollower()
	}
	//todo when '='
	if reply.VoteGranted && rf.peerKind == Candidate && rf.currentTerm == term {
		rf.mu.Lock()
		rf.votesGained += 1
		rf.mu.Unlock()
	}

}
func (rf *Raft) sendAppendEntries(server int) {

	args := AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, rf.commitIndex}
	reply := AppendEntriesReply{}
	if !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
		//DPrintf("Leader(state: %v) %v fail to send AppendEntries(heartbeat) rpc to server %v\n", rf.peerKind, rf.me, server)
		return
	}
	if reply.Term > rf.currentTerm && rf.peerKind == Leader {
		DPrintf("Leader %v downgrade when send AE rpc to server %v, currentTerm %v\n", rf.me, server, rf.currentTerm)
		rf.TurnFollower()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//heartbeat
	if len(args.Entries) == 0 {
		rf.mu.Lock()
		if rf.peerKind == Leader || rf.peerKind == Candidate {
			rf.peerKind = Follower
		}
		rf.votedFor = -1
		rf.lastUpdated = time.Now()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Act() {
	ElectionTimeout := time.Duration(rand.Int()%ElectionTimeoutBase+ElectionTimeoutBase) * time.Millisecond
	peersSum := len(rf.peers)
	for !rf.killed() {
		switch rf.peerKind {
		case Follower:
			if time.Now().Sub(rf.lastUpdated) > ElectionTimeout {
				DPrintf("Candidate %v started election after %v, term: %v\n", rf.me, ElectionTimeout, rf.currentTerm+1)
				ElectionTimeout = time.Duration(rand.Int()%ElectionTimeoutBase+ElectionTimeoutBase) * time.Millisecond
				go rf.StartElection()
			}
			time.Sleep(CheckTimeoutDuration)
		case Candidate:
			if time.Now().Sub(rf.lastUpdated) > ElectionTimeout {
				DPrintf("Candidate %v election timeout after %v, term: %v\n", rf.me, ElectionTimeout, rf.currentTerm)
				ElectionTimeout = time.Duration(rand.Int()%ElectionTimeoutBase+ElectionTimeoutBase) * time.Millisecond
				rf.TurnFollower()
				continue
			}
			if rf.votesGained >= peersSum-peersSum/2 {
				DPrintf("Server %v becomes the leader\n", rf.me)
				rf.mu.Lock()
				rf.peerKind = Leader
				rf.votedFor = -1
				rf.mu.Unlock()
				continue
			}
			time.Sleep(CheckTimeoutDuration) //todo
		case Leader:
			for index := 0; index < len(rf.peers); index++ {
				if index != rf.me {
					go rf.sendAppendEntries(index)

				}
			}
			time.Sleep(HeartBeatTimeout)
		default:
			log.Fatal("Bad peerKind: ", rf.peerKind)
		}

	}
}

//
// Become a candidate and start an election
//
func (rf *Raft) StartElection() {

	rf.mu.Lock()
	rf.peerKind = Candidate
	rf.currentTerm += 1
	rf.votesGained = 1
	rf.votedFor = rf.me
	rf.lastUpdated = time.Now()
	rf.mu.Unlock()

	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			// use goroutine to send RPCs separately
			go rf.sendRequestVote(index)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.logs
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.peerKind = Follower
	rf.lastUpdated = time.Now()
	rf.votesGained = 0

	go rf.Act()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
