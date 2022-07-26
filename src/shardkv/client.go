package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

const (
	debug = true
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	SeqId    int
	ClientId int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.SeqId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.SeqId++
	cnt := 0
	tim := time.Now()
	for {
		cnt++

		args := GetArgs{key, ck.SeqId, ck.ClientId}
		args.Key = key
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		//fmt.Println(gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					//fmt.Println("return")
					return reply.Value
				}
				if time.Since(tim) > 10*time.Second {
					if debug {
						fmt.Println(ok, ck.config, ck.ClientId, ck.SeqId, servers[si], "start get", " ", shard, "  ", gid, "  ", reply.Err)
					}
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.SeqId++
	tim := time.Now()
	for {

		args := PutAppendArgs{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.SeqId = ck.SeqId
		args.ClientId = ck.ClientId
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		//fmt.Println(gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				//fmt.Println(1)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				//fmt.Println(2)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if time.Since(tim) > 10*time.Second {
					if debug {
						fmt.Println(ok, ck.config, ck.ClientId, ck.SeqId, "start ", op, reply.Err)
					}
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//fmt.Println(1)
		ck.config = ck.sm.Query(-1)
		//fmt.Println(2)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
