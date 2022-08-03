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
	//	"bytes"

	//"fmt"

	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower = iota
	Leader
	Candidate
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Command interface{}
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	state       int
	timeout     time.Time
	leader      int

	log         []LogEntry
	commitindex int
	lastApplied int
	nextindex   []int
	matchindex  []int

	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	//fmt.Println("save", rf.me, rf.log)

	data := w.Bytes()
	//fmt.Println(data)
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		//fmt.Printf("RaftNode[%d] persist read, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, currentTerm, votedFor, logs)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	// Your data here (2A, 2B).
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.Refresh(args.Term)
	}
	if rf.votedFor != -1 || rf.state == Leader {
		reply.VoteGranted = false
	} else {
		//fmt.Println(rf.me, args.CandidateId)
		//fmt.Println(rf.log[len(rf.log)-1].Term, args.LastLogTerm, len(rf.log)-1, args.LastLogIndex)
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm || rf.log[len(rf.log)-1].Term == args.LastLogTerm && (len(rf.log)-1 > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}
}

func (rf *Raft) Refreshtime() {
	ms := 150 + (rand.Int() % 150)
	str := strconv.Itoa(ms)
	str = str + "ms"
	//fmt.Println(str)
	dur, _ := time.ParseDuration(str)
	rf.timeout = time.Now().Add(dur)
	//fmt.Println(rf.timeout)
}
func (rf *Raft) Refresh(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
	rf.Refreshtime()
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == true {
		return -1, -1, false
	}
	if rf.state != Leader {
		return -1, -1, false
	} else {

		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index := len(rf.log) - 1
		term := rf.currentTerm
		rf.persist()
		return index, term, true
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		//fmt.Println(rf.me, rf.log)
		//fmt.Println(rf.me)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.state == Leader {
			//fmt.Println(rf.me, "现在是领导者在纪元", rf.currentTerm)
			//fmt.Println(rf.me, "领导者在纪元:", rf.currentTerm, "日志状态:", rf.log)
			rf.mu.Lock()
			rf.Heartbeats()
			rf.mu.Unlock()
		} else {
			//fmt.Println(rf.me, "跟随者在纪元:", rf.currentTerm, "日志状态:", rf.log)
			if time.Now().After(rf.timeout) {
				rf.Xuanju()
			}
		}
		time.Sleep(time.Second / 20)
	}
}
func (rf *Raft) appendticker() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.leaderappend()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
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
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0})
	rf.matchindex = make([]int, len(peers))
	rf.nextindex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextindex[i] = 1
	}
	rf.commitindex = 0
	rf.lastApplied = 0
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.Refreshtime()
	rf.currentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyChan = applyCh
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.appendticker()
	go rf.committedTicker()
	return rf
}

func (rf *Raft) leaderappend() {
	currentTerm := rf.currentTerm
	me := rf.me
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		prevLogIndex := rf.nextindex[i] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		//fmt.Println(me, "sent heart to", i, "term:", currentTerm)
		args := &AppendEntriesArgs{AppendEntries, currentTerm, me, prevLogIndex, prevLogTerm, nil, min(rf.commitindex, rf.matchindex[i])}
		reply := &AppendEntriesReply{}
		go rf.SendAppendEntries(i, args, reply)
	}
}
func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for rf.killed() == false {
		time.Sleep(15 * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitindex {
			rf.mu.Unlock()
			continue
		}
		//fmt.Println(rf.me, rf.lastApplied, rf.commitindex)
		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitindex && rf.lastApplied < len(rf.log)-1 {
			rf.lastApplied += 1
			//fmt.Println(rf.me, "已经提交", rf.lastApplied, rf.log[rf.lastApplied].Command)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.log[rf.lastApplied].Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}
