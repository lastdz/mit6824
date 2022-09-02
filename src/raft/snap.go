package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.Term {
		rf.Refresh(args.Term)
		rf.votedFor = args.LeaderID
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if rf.state != Follower {
		rf.Refresh(args.Term)
		rf.votedFor = args.LeaderID
		rf.persist()
	}
	if rf.base >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludedIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})
	for i := index + 1; i <= rf.getlastindex(); i++ {
		tempLog = append(tempLog, rf.getLog(i))
	}

	rf.lastterm = args.LastIncludedTerm
	rf.base = args.LastIncludedIndex

	rf.log = tempLog
	if rf.lastApplied >= args.LastIncludedIndex {
		rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
		rf.mu.Unlock()
		return
	}
	if index > rf.commitindex {
		rf.commitindex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	//fmt.Println(rf.me, "收到快照,更新commit", rf.commitindex)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastterm,
		SnapshotIndex: rf.base,
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	rf.mu.Unlock()
	rf.applyChan <- msg
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.base || index > rf.commitindex {
		return
	}
	Log := make([]LogEntry, 0)
	Log = append(Log, LogEntry{0, 0})
	for i := index + 1; i <= rf.getlastindex(); i++ {
		Log = append(Log, rf.getLog(i))
	}
	if index == rf.getlastindex() {
		rf.lastterm = rf.getlastTerm()
	} else {
		rf.lastterm = rf.getTerm(index)
	}
	//fmt.Println(rf.me, "快照", rf.log)
	rf.base = index
	rf.log = Log
	//fmt.Println(rf.log)
	if index > rf.commitindex {
		rf.commitindex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}
func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.base,
		rf.lastterm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.Refresh(reply.Term)
			rf.mu.Unlock()
			return
		}

		rf.matchindex[server] = args.LastIncludedIndex
		rf.nextindex[server] = args.LastIncludedIndex + 1

		rf.mu.Unlock()
		return
	}
}
