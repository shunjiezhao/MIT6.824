package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	name       string
	lastExec   map[ClientID]int64             // 1
	resultChan map[int]chan OpReply           // 2
	Result     map[ClientID]map[int64]OpReply // 6
	Shards     map[int][]int
}

const (
	Join uint8 = iota + 1
	Leave
	Move
	Query
)

func copyMap(Servers map[int][]string) map[int][]string {
	var ans = make(map[int][]string, len(Servers))
	for key, val := range Servers {
		var slic = make([]string, len(val))
		copy(slic, val)
		ans[key] = slic
	}
	return ans
}

func (sc *ShardCtrler) Op(args *OpArgs, reply *OpReply) {
	if args == nil || reply == nil {
		panic("args or reply is nil")
	}
	sc.Lock("OP")
	defer func() {
		Debug(sc, dInfo, "OP args: %s reply: %v", args, reply)
	}()
	if _, ok := sc.Result[args.ClientID]; !ok {
		sc.Result[args.ClientID] = map[int64]OpReply{}
	}
	// 1. check if already executed
	clientRes := sc.Result[args.ClientID]
	if res, ok := clientRes[args.SeqNum]; ok {
		reply.Err = res.Err
		reply.Config = res.Config
		reply.WrongLeader = res.WrongLeader
		Debug(sc, dInfo, "repeat")
		sc.UnLock("OP-result")
		return
	}

	// Your code here.
	Debug(nil, dInfo, "Op args: %s", args)
	index, _, leader := sc.rf.Start(*args)
	if !leader {
		reply.WrongLeader = true
		sc.UnLock("OP-wrong-leader")
		return
	}

	ch := sc.resultChan[index]
	if ch == nil {
		ch = make(chan OpReply)
		sc.resultChan[index] = ch
	}
	panicIf(ch == nil, "ch is nil")
	sc.UnLock("Join-wait-result")
	// 3. wait for raft to apply
	select {
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimedOut
		reply.WrongLeader = true
	case result := <-ch:
		reply.Err = result.Err
		reply.Config = result.Config
		reply.WrongLeader = result.WrongLeader
		//reply = &result
	}
	// 4. return result
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(OpArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.name = fmt.Sprintf("SC-%d", me)

	// Your code here.
	sc.lastExec = map[ClientID]int64{}               // 1
	sc.resultChan = make(map[int]chan OpReply)       // 2
	sc.Result = make(map[ClientID]map[int64]OpReply) // 6
	sc.Shards = make(map[int][]int)
	Debug(sc, dInfo, "StartServer me: %v", me)

	go sc.apply()
	return sc
}

func (sc *ShardCtrler) Lock(locate string) {
	micro := time.Now().UnixMicro()
	Debug(sc, dLock, "Pre-Lock: %v on %v", locate, micro)
	sc.mu.Lock()
	Debug(sc, dLock, "Done-Lock: %v on %v", locate, micro)
}
func (sc *ShardCtrler) UnLock(locate string) {
	micro := time.Now().UnixMicro()
	Debug(sc, dLock, "Pre-UnLock: %v on %v", locate, micro)
	sc.mu.Unlock()
	Debug(sc, dLock, "Done-UnLock: %v on %v", locate, micro)
}
