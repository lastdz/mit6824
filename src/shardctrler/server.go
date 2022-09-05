package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const De = false

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	seqMap    map[int64]int
	waitChMap map[int]chan Op
	killch    chan bool
}

type Op struct {
	// Your data here.
	Optype   string
	Args     interface{}
	ClientId int64
	SeqId    int
	Index    int
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	sc.killch <- true
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	op := Op{Optype: "Join", Args: *args, ClientId: args.ClientId, SeqId: args.SeqId}
	lastindex, _, _ := sc.rf.Start(op)
	if lastindex == -1 {
		reply.WrongLeader = true
		return
	}
	ch := sc.getWaitCh(lastindex)
	op.Index = lastindex
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyop := <-ch:
		if op.ClientId != replyop.ClientId || op.SeqId != replyop.SeqId {
			//fmt.Println(op.ClientId, "   ", replyop.ClientId, " ", op.SeqId, "  ", replyop.SeqId)
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	case <-timer.C:
		//fmt.Println("j超时")
		reply.WrongLeader = true
		reply.Err = "TimeOut"
	}
}
func (sc *ShardCtrler) createNextConfig() Config {
	lastCfg := sc.configs[len(sc.configs)-1]
	nextCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	op := Op{Optype: "Leave", Args: *args, ClientId: args.ClientId, SeqId: args.SeqId}
	lastindex, _, _ := sc.rf.Start(op)
	if lastindex == -1 {
		reply.WrongLeader = true
		return
	}
	ch := sc.getWaitCh(lastindex)
	op.Index = lastindex
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyop := <-ch:
		if op.ClientId != replyop.ClientId || op.SeqId != replyop.SeqId {
			reply.WrongLeader = true
		} else {
			reply.Err = OK

		}
	case <-timer.C:
		//.Println("l超时")
		reply.WrongLeader = true
		reply.Err = "TimeOut"
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	op := Op{Optype: "Move", Args: *args, ClientId: args.ClientId, SeqId: args.SeqId}
	lastindex, _, _ := sc.rf.Start(op)
	if lastindex == -1 {
		reply.WrongLeader = true
		return
	}
	ch := sc.getWaitCh(lastindex)
	op.Index = lastindex
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyop := <-ch:
		if op.ClientId != replyop.ClientId || op.SeqId != replyop.SeqId {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		//fmt.Println("m超时")
		reply.WrongLeader = true
		reply.Err = "TimeOut"
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	op := Op{Optype: "Query", ClientId: args.ClientId, SeqId: args.SeqId}
	lastindex, _, _ := sc.rf.Start(op)
	if lastindex == -1 {
		reply.WrongLeader = true
		return
	}
	ch := sc.getWaitCh(lastindex)
	op.Index = lastindex
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyop := <-ch:
		if op.ClientId != replyop.ClientId || op.SeqId != replyop.SeqId {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			sc.mu.Lock()

			if args.Num >= 0 && args.Num < len(sc.configs) {
				reply.Config = sc.configs[args.Num]
			} else {
				reply.Config = sc.configs[len(sc.configs)-1]
			}
			sc.mu.Unlock()
		}
	case <-timer.C:
		//fmt.Println("q超时")
		reply.WrongLeader = true
		reply.Err = "TimeOut"
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChMap = make(map[int]chan Op)
	sc.seqMap = make(map[int64]int)
	sc.killch = make(chan bool, 1)
	go sc.applyMsgHandlerLoop()
	return sc
}
func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}
func (sc *ShardCtrler) rebalance(cfg *Config, request string, gid int) {
	shardsCount := sc.groupByGid(cfg)
	switch request {
	case "Join":
		avg := NShards / len(cfg.Groups)
		for i := 0; i < avg; i++ {
			maxGid := sc.getMaxShardGid(shardsCount)
			cfg.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case "Leave":
		shardsArray, exists := shardsCount[gid]
		if !exists {
			return
		}
		delete(shardsCount, gid)
		if len(cfg.Groups) == 0 { // remove all gid
			cfg.Shards = [NShards]int{}
			return
		}
		for _, v := range shardsArray {
			minGid := sc.getMinShardGid(shardsCount)
			cfg.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid], v)
		}
	}
}
func (sc *ShardCtrler) groupByGid(cfg *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k, _ := range cfg.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}
func (sc *ShardCtrler) getMaxShardGid(shardsCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardsCount {
		if max < len(v) {
			max = len(v)
			gid = k
		} else if max == len(v) && gid < k {
			gid = k
		}
	}
	return gid
}
func (sc *ShardCtrler) getMinShardGid(shardsCount map[int][]int) int {
	min := 99999999
	var gid int
	for k, v := range shardsCount {
		if min > len(v) {
			min = len(v)
			gid = k
		} else if min == len(v) && gid < k {
			gid = k
		}
	}
	return gid
}
func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		//fmt.Println("************", sc.me, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
		select {
		case <-sc.killch:
			//fmt.Println(sc.me, "killed")
			return
		case msg := <-sc.applyCh:

			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if De && op.Optype != "Query" {

					fmt.Println(1, op.Optype)
				}

				if op.Optype == "Query" {
					//fmt.Println(213123123)
					sc.getWaitCh(index) <- op
					continue
				}
				if De {
					fmt.Println(2, op.Optype)
					fmt.Println(op.Optype, "    ", op.ClientId, "   ", op.SeqId, "    ", sc.seqMap[op.ClientId])
				}

				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					sc.seqMap[op.ClientId] = op.SeqId
					switch op.Optype {
					case "Move":
						arg := op.Args.(MoveArgs)
						shard := arg.Shard
						gid := arg.GID
						cfg := sc.createNextConfig()
						_, exist := cfg.Groups[gid]
						if exist {
							cfg.Shards[shard] = gid
							sc.configs = append(sc.configs, cfg)
						} else {
							continue
						}
					case "Join":
						joinArg := op.Args.(JoinArgs)
						tmp := []int{}
						for gid, _ := range joinArg.Servers {
							tmp = append(tmp, gid)
						}
						sort.Ints(tmp)
						for _, gid := range tmp {
							servers := joinArg.Servers[gid]
							newServers := make([]string, len(servers))
							copy(newServers, servers)
							cfg := sc.createNextConfig()
							cfg.Groups[gid] = newServers
							sc.rebalance(&cfg, "Join", gid)
							sc.configs = append(sc.configs, cfg)
						}
					case "Leave":
						LeaveArg := op.Args.(LeaveArgs)
						for _, gid := range LeaveArg.GIDs {
							cfg := sc.createNextConfig()
							delete(cfg.Groups, gid)
							sc.rebalance(&cfg, "Leave", gid)
							sc.configs = append(sc.configs, cfg)
						}
					}

					sc.mu.Unlock()
					//fmt.Println(213123123)
					if De {
						fmt.Println(op.Optype)
					}

					sc.getWaitCh(index) <- op
				}
			}
		}
	}
}
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
