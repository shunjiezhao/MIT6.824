package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastExec      map[ClientID]LastOpInfo
	ResponseCh    map[int]chan OpReply
	store         map[int]*Shard // Shard -> Store
	sm            *shardctrler.Clerk
	preCfg        shardctrler.Config // isNew?
	curCfg        shardctrler.Config
	persister     *raft.Persister
	kill          atomic.Int64
	snapShotIndex int
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.kill.Store(1)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific Shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Shard{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(OpArgs{})
	labgob.Register(OpReply{})
	labgob.Register(MoveShardArgs{})
	labgob.Register(MoveShardReply{})
	labgob.Register(CMDConfigArgs{})
	labgob.Register(CMDMoveShardArgs{})
	labgob.Register(AckArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastExec = make(map[ClientID]LastOpInfo)
	kv.ResponseCh = make(map[int]chan OpReply)
	kv.store = make(map[int]*Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.store[i] = NewShard()
	}

	kv.curCfg = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.curCfg.Shards[i] = -1
	}
	kv.preCfg = kv.curCfg

	kv.start()
	snapshot := persister.ReadSnapshot()
	kv.InstallSnapshot(snapshot)
	return kv
}
func (kv *ShardKV) start() {
	go kv.Deamon(kv.PullConfig, false)
	go kv.Deamon(kv.shardConsumer, true)
	go kv.Deamon(kv.GCconsumer, true)
	go kv.apply()
}

func (sc *ShardKV) Lock(locate string) {
	micro := time.Now().UnixMicro()
	Debug(sc, dLock, "Pre-Lock: %v on %v", locate, micro)
	sc.mu.Lock()
	Debug(sc, dLock, "Done-Lock: %v on %v", locate, micro)
}
func (sc *ShardKV) UnLock(locate string) {
	micro := time.Now().UnixMicro()
	Debug(sc, dLock, "Pre-UnLock: %v on %v", locate, micro)
	sc.mu.Unlock()
	Debug(sc, dLock, "Done-UnLock: %v on %v", locate, micro)
}

func (kv *ShardKV) canServe(shard int) bool {
	return kv.curCfg.Shards[shard] == kv.gid && (kv.store[shard].Status == Serveing || kv.store[shard].Status == GC)

}

func (kv *ShardKV) killed() bool {
	return kv.kill.Load() == 1
}

func (kv *ShardKV) isDuplicateRequest(id ClientID, seqNum int64) bool {
	if info, ok := kv.lastExec[id]; ok {
		return info.SeqNum >= seqNum
	}
	return false
}
