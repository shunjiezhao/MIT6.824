package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's Shards.
//

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which Shard is a key in?
// please use this function,
// and please do not change it.
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
	SeqNum atomic.Int64
	ClientID
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.ClientID = ClientID(nrand())
	return ck
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) Op(key string, value string, op string) string {
	args := OpArgs{}
	args.BaseReq = BaseReq{
		ClientID: ck.ClientID,
		SeqNum:   ck.SeqNum.Add(1),
	}
	args.Key = key
	args.Value = value
	args.Type = op

	for {
		shard := key2shard(key)
		args.Shard = shard
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply OpReply
				ok := srv.Call("ShardKV.Op", &args, &reply)
				if ok && reply.Err == OK {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					Debug(nil, dErr, "WrongGroup: %v %v", reply.Err, ck.config.Num)
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.Op(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Op(key, value, Append)
}
func (ck *Clerk) Get(key string) string {
	return ck.Op(key, "", Get)
}
