package raft

import (
	"time"
)

func (rf *Raft) prevote() bool {
	rf.mu.Lock()
	me := rf.me
	currentTerm := rf.currentTerm + 1
	cnt := 1
	flag := 0
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		//fmt.Println(me, "sent xuanju to", i, "term :", currentTerm)
		lastLogIndex := rf.getlastindex()
		lastLogTerm := rf.getTerm(lastLogIndex)
		go func(a int) {
			arg := &RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm, true}
			reply := &RequestVoteReply{}
			//fmt.Println(me, "send xuanju to", a, "term :", currentTerm)
			ok := rf.SendRequestVote(a, arg, reply)
			rf.mu.Lock()
			if ok == false {
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted == true {
				//fmt.Println(me, "receive xuanju from", a, "term :", currentTerm)
				cnt++
				if cnt > len(rf.peers)/2 {
					flag = 1
				}
			}
			rf.mu.Unlock()
		}(i)
	}

	ran, _ := time.ParseDuration("100ms")
	f := time.Now().Add(ran)
	for {
		rf.mu.Lock()
		//fmt.Println(cnt, "  ", flag, " ", currentTerm, " ", rf.currentTerm)
		if flag == 1 && currentTerm == rf.currentTerm+1 {
			rf.mu.Unlock()
			return true
		}
		if time.Now().After(f) {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()
		time.Sleep(time.Second / 100)
	}
}
func (rf *Raft) Xuanju() {
	rf.mu.Lock()
	//fmt.Println(rf.me, "become candidate", "term :", rf.currentTerm)
	rf.currentTerm++
	rf.Refresh(rf.currentTerm)
	currentTerm := rf.currentTerm
	me := rf.me
	cnt := 1
	flag := 0
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		//fmt.Println(me, "sent xuanju to", i, "term :", currentTerm)
		lastLogIndex := rf.getlastindex()
		lastLogTerm := rf.getTerm(lastLogIndex)
		go func(a int) {
			arg := &RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm, false}
			reply := &RequestVoteReply{}
			//fmt.Println(me, "send xuanju to", a, "term :", currentTerm)
			ok := rf.SendRequestVote(a, arg, reply)
			rf.mu.Lock()
			if ok == false {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.Refresh(reply.Term)
			}
			if reply.VoteGranted == true {
				//fmt.Println(me, "receive xuanju from", a, "term :", currentTerm)
				cnt++
				if cnt > len(rf.peers)/2 {
					flag = 1
				}
			}
			rf.mu.Unlock()
		}(i)
	}

	ran, _ := time.ParseDuration("100ms")
	f := time.Now().Add(ran)
	for {
		rf.mu.Lock()
		//fmt.Println("xuanju", cnt, "  ", flag, " ", currentTerm, " ", rf.currentTerm)
		if flag == 1 && currentTerm == rf.currentTerm {
			//fmt.Println(rf.me, "become leader term:", rf.currentTerm)
			//fmt.Println(rf.me, "变成领导者在纪元:", rf.currentTerm, "日志状态:", rf.log)
			rf.state = Leader
			rf.persist()

			for i := 0; i < len(rf.peers); i++ {
				rf.matchindex[i] = 0
				rf.nextindex[i] = rf.getlastindex() + 1
			}
			rf.Heartbeats()
			rf.mu.Unlock()
			return
		}
		if time.Now().After(f) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Second / 100)
	}
}
