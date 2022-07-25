package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if rf.currentTerm < reply.Term {
			rf.Refresh(reply.Term)
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//fmt.Println(rf.me, "term", rf.currentTerm, "recevice heart from", args.LeaderID, "term", args.Term)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	} else {
		rf.currentTerm = args.Term
		rf.Refresh(rf.currentTerm)
	}
	rf.leader = args.LeaderID
}
func (rf *Raft) Heartbeats() {
	currentTerm := rf.currentTerm
	me := rf.me
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		//fmt.Println(me, "sent heart to", i, "term:", currentTerm)
		args := &AppendEntriesArgs{currentTerm, me}
		reply := &AppendEntriesReply{}
		go rf.SendAppendEntries(i, args, reply)
	}
}

