package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
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
	args := &QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
	ck.seqId++
	// Your code here.
	args.Num = num
	for {
		reply := &QueryReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply)
		//fmt.Println(res, reply.WrongLeader)
		if res && !reply.WrongLeader {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	ck.seqId++

	for {
		// try each known server.
		reply := &JoinReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply)
		if res && !reply.WrongLeader {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	ck.seqId++

	for {
		// try each known server.
		reply := &LeaveReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply)
		if res && !reply.WrongLeader {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	ck.seqId++

	for {
		// try each known server.
		reply := &MoveReply{}
		res := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply)
		if res && !reply.WrongLeader {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
