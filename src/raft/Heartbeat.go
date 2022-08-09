package raft

const (
	Heart = iota
	AppendEntries
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

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

		if rf.nextindex[server] <= rf.getlastindex() {
			for rf.nextindex[server] <= rf.getlastindex() {
				if rf.state != Leader {
					rf.mu.Unlock()
					return true
				}
				if args.PrevLogIndex < rf.base {
					go rf.leaderSendSnapShot(server)
					rf.mu.Unlock()
					return true
				}

				args.PrevLogTerm = rf.getTerm(args.PrevLogIndex)
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
						if args.PrevLogIndex < rf.base {
							go rf.leaderSendSnapShot(server)
							rf.mu.Unlock()
							return true
						}
						//fmt.Println(server, "成功复制到", len(rf.log))
						//fmt.Println(rf.me, args.PrevLogIndex, rf.base)
						args.PrevLogTerm = rf.getTerm(args.PrevLogIndex)
						args.Entries = make([]LogEntry, 0)
						for i := args.PrevLogIndex + 1; i <= rf.getlastindex(); i++ {
							args.Entries = append(args.Entries, rf.getLog(i))
						}
						limit := rf.getlastindex() + 1
						rf.mu.Unlock()
						ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
						rf.mu.Lock()
						if ok {
							if reply.Success == true {
								//fmt.Println(server, "changeed", "at term", rf.currentTerm)
								//fmt.Println(rf.matchindex[server])
								//fmt.Println(len(rf.log))
								rf.nextindex[server] = max(limit, rf.nextindex[server])
								rf.matchindex[server] = rf.nextindex[server] - 1
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
		for i := rf.getlastindex(); i > rf.commitindex; i-- {
			if rf.getTerm(i) != rf.currentTerm {
				break
			}
			tmp := 1
			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchindex[j] >= i {
					tmp++
				}
			}
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
			if rf.currentTerm < args.Term {
				rf.Refresh(args.Term)
				rf.votedFor = args.LeaderID
				rf.persist()
			} else {
				rf.currentTerm = args.Term
				rf.votedFor = args.Term
				rf.state = Follower
				rf.persist()
				rf.Refreshtime()
			}
		}
		if args.LeaderCommit > rf.commitindex {
			rf.commitindex = min(args.LeaderCommit, rf.getlastindex())
		}
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

		previ := args.PrevLogIndex
		prevt := args.PrevLogTerm
		if rf.getlastindex() >= previ {
			if previ >= rf.base && rf.getTerm(previ) == prevt {
				if len(args.Entries) > 0 {
					//fmt.Println(rf.me, args.Entries)
					len1 := len(args.Entries) + args.PrevLogIndex
					len2 := rf.getlastindex()
					if len1 <= len2 && rf.leader == args.LeaderID {
						reply.Success = false
						return
					}
					tmplog := make([]LogEntry, 0)
					tmplog = append(tmplog, LogEntry{0, 0})
					for i := rf.base + 1; i <= previ; i++ {
						tmplog = append(tmplog, rf.getLog(i))
					}
					rf.log = append(tmplog, args.Entries...)
					//fmt.Println(rf.me, "复制了", args.LeaderID, len(rf.log), "当前commitindex", rf.commitindex)
					rf.persist()
					if args.LeaderCommit > rf.commitindex {
						rf.commitindex = min(args.LeaderCommit, rf.getlastindex())
					}
					//fmt.Println(rf.me, "复制了", args.Entries)
					rf.leader = args.LeaderID
				}
				reply.Success = true

			} else {
				reply.Success = false
				rf.persist()
			}
			return
		}
		if rf.getlastindex() < previ {
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
		prevLogIndex := 0
		prevLogTerm := 0
		args := &AppendEntriesArgs{Heart, currentTerm, me, prevLogIndex, prevLogTerm, nil, min(rf.commitindex, rf.matchindex[i])}
		reply := &AppendEntriesReply{}
		go rf.SendAppendEntries(i, args, reply)
	}
}
