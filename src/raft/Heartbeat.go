package raft

import "fmt"

const (
	Heart = iota
	AppendEntries
)

type AppendEntriesArgs struct {
	Is           int
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Index   int
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if args.Is == Heart {
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		if ok {
			if rf.currentTerm < reply.Term {
				rf.Refresh(reply.Term)
			}
		}
		rf.mu.Unlock()
		return ok
	} else if args.Is == AppendEntries {
		
		if rf.nextindex[server] <= len(rf.log)-1 {
			for rf.nextindex[server] <= len(rf.log)-1 {
				if rf.state != Leader {
					rf.mu.Unlock()
					return true
				}
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = nil
				rf.mu.Unlock()
				ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return false
				}
				if ok {
					if rf.currentTerm < reply.Term {
						rf.Refresh(reply.Term)
						rf.mu.Unlock()
						return false
					}
					if reply.Success == false {
						if rf.currentTerm < reply.Term {
							rf.Refresh(reply.Term)
							rf.mu.Unlock()
							return false
						}
						args.PrevLogIndex = reply.Index - 1
						rf.nextindex[server] = args.PrevLogIndex + 1
					} else {
						//fmt.Println(server, "成功复制到", len(rf.log))
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = rf.log[args.PrevLogIndex+1:]
						limit := len(rf.log)
						rf.mu.Unlock()
						ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
						rf.mu.Lock()
						if ok {
							if reply.Success == true {
								fmt.Println(server, "changeed", "at term", rf.currentTerm)
								fmt.Println(rf.matchindex[server])
								fmt.Println(len(rf.log))
								rf.nextindex[server] = limit
								rf.matchindex[server] = limit - 1
								//fmt.Println(rf.matchindex[server])
							}
						}
						break
					}
				} else {
					rf.mu.Unlock()
					return false
				}
			}
		}
		for i := len(rf.log) - 1; i > rf.commitindex; i-- {
			tmp := 1
			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchindex[j] >= i {
					tmp++
				}
			}
			//fmt.Println(i, tmp)
			if tmp > len(rf.peers)/2 {
				rf.commitindex = i
				//fmt.Println(i)
				break
			}
		}
	}
	rf.mu.Unlock()
	return true
}
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//fmt.Println(rf.me, "term", rf.currentTerm, "recevice heart from", args.LeaderID, "term", args.Term)
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Index = rf.lastApplied + 1
	if args.Is == Heart {
		//fmt.Println(rf.me, rf.log)
		reply.Term = rf.currentTerm

		if rf.currentTerm > args.Term {
			return
		} else {
			rf.currentTerm = args.Term
			rf.Refresh(rf.currentTerm)
		}
		if args.LeaderCommit > rf.commitindex {
			rf.commitindex = min(args.LeaderCommit, len(rf.log)-1)
		}
		rf.leader = args.LeaderID
		reply.Success = true
	} else if args.Is == AppendEntries {
		//fmt.Println(args.PrevLogIndex, args.PrevLogTerm, len(rf.log)-1)
		reply.Term = rf.currentTerm
		if rf.currentTerm > args.Term {
			return
		} else {
			if rf.currentTerm < args.Term {
				rf.Refresh(args.Term)
				rf.votedFor = args.LeaderID
				rf.persist()
			}
		}

		rf.leader = args.LeaderID
		previ := args.PrevLogIndex
		prevt := args.PrevLogTerm
		if len(rf.log)-1 >= previ {
			if rf.log[previ].Term == prevt {
				reply.Success = true
				if len(args.Entries) > 0 {
					//fmt.Println(rf.me, args.Entries)
					rf.log = rf.log[:previ+1]
					rf.log = append(rf.log, args.Entries...)
					fmt.Println(rf.me, "复制了", args.LeaderID, len(rf.log), "当前commitindex", rf.commitindex)
					rf.persist()
					if args.LeaderCommit > rf.commitindex {
						rf.commitindex = min(args.LeaderCommit, len(rf.log)-1)
					}
				}

			} else {
				rf.log = rf.log[:previ]
				reply.Success = false
				rf.persist()
			}
			return
		}
		if len(rf.log)-1 < previ {
			reply.Success = false
		}
	}
}
func (rf *Raft) Heartbeats() {
	currentTerm := rf.currentTerm
	me := rf.me
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		//fmt.Println(me, "sent heart to", i, "term:", currentTerm)
		prevLogIndex := rf.nextindex[i] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		args := &AppendEntriesArgs{Heart, currentTerm, me, prevLogIndex, prevLogTerm, nil, min(rf.commitindex, rf.matchindex[i])}
		reply := &AppendEntriesReply{}
		go rf.SendAppendEntries(i, args, reply)
	}
}
