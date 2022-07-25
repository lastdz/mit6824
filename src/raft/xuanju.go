package raft

import (
	//"fmt"
	"time"
)

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
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			continue
		}
		//fmt.Println(me, "sent xuanju to", i, "term :", currentTerm)
		go func(a int) {
			arg := &RequestVoteArgs{currentTerm, me}
			reply := &RequestVoteReply{}
			rf.SendRequestVote(a, arg, reply)
			rf.mu.Lock()
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
		if flag == 1 && currentTerm == rf.currentTerm {
			//fmt.Println(rf.me, "become leader term:", rf.currentTerm)
			rf.state = Leader
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

