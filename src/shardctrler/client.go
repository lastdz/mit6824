package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	mathrand "math/rand"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
	leaderId int // 确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqId++

	// Your code here.
	// tim := time.Now()
	for {

		args := &QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
		reply := &QueryReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply)
		//fmt.Println(res, reply.WrongLeader)
		if res && !reply.WrongLeader {
			return reply.Config
		}
		if res && reply.Err == "ErrKilled" {
			ck.seqId++
		}
		// if time.Since(tim) > 10*time.Second {
		// 	fmt.Println(ck.clientId, ck.seqId, "start query", "    ", reply.Err)
		// 	tim = time.Now()
		// }
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++

	// Your code here.
	// tim := time.Now()
	for {
		// if time.Since(tim) > 10*time.Second {
		// 	fmt.Println(ck.clientId, ck.seqId, "start join")
		// 	tim = time.Now()
		// }
		// try each known server.
		args := &JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
		reply := &JoinReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply)
		//fmt.Println("join", ck.leaderId, res, reply.WrongLeader)
		if res && !reply.WrongLeader {
			return
		}
		if res && reply.Err == "ErrKilled" {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++

	// Your code here.
	tim := time.Now()
	for {
		if time.Since(tim) > 10*time.Second {
			fmt.Println(ck.clientId, ck.seqId, "start leave")
			tim = time.Now()
		}
		// try each known server.
		args := &LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
		reply := &LeaveReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply)
		//fmt.Println("leave", ck.leaderId, res, reply.WrongLeader)
		if res && !reply.WrongLeader {
			return
		}
		if res && reply.Err == "ErrKilled" {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++

	// Your code here.

	for {
		// try each known server.
		args := &MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
		reply := &MoveReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply)
		//fmt.Println("move", ck.leaderId, res, reply.WrongLeader)
		if res && !reply.WrongLeader {
			return
		}
		if res && reply.Err == "ErrKilled" {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
