package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type ClientID int64
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqNum atomic.Int64

	me        ClientID
	newConfig Config
	next      int
	leaderId  int
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
	// Your code here.
	ck.me = ClientID(nrand())
	Debug(nil, dInfo, "MakeClerk: %v", ck)
	return ck
}
func (ck *Clerk) buildBaseReq() BaseReq {
	return BaseReq{
		ClientID: ck.me,
		SeqNum:   ck.seqNum.Add(1),
	}
}
func (ck *Clerk) Query(num int) Config {
	args := &OpArgs{
		BaseReq: ck.buildBaseReq(),
		Num:     num,
	}
	args.Type = Query
	// Your code here.
	return ck.op(args).Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &OpArgs{
		BaseReq: ck.buildBaseReq(),
		Servers: servers,
	}
	// Your code here.
	args.Type = Join
	Debug(nil, dInfo, "Join args: %v", args)
	ck.op(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &OpArgs{
		BaseReq: ck.buildBaseReq(),
		GIDs:    gids,
	}
	// Your code here.
	args.Type = Leave

	ck.op(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &OpArgs{
		BaseReq: ck.buildBaseReq(),
		Shard:   shard,
		GID:     gid,
	}
	// Your code here.
	args.Type = Move
	ck.op(args)
}

func (ck *Clerk) op(args *OpArgs) OpReply {

	for {
		var reply OpReply

		Debug(nil, dClient, "call %d server %s begin", ck.getNext(), args)

		var call bool
		call = ck.servers[ck.getNext()].Call("ShardCtrler.Op", args, &reply)
		// You will have to modify this function.
		if !call {
			Debug(nil, dWarn, "call %v false", ck.getNext())
			ck.refreshNext()
			continue
		}

		Debug(nil, dInfo, "%d call args: %s reply %s", ck.getNext(), args, reply)
		switch reply.Err {
		case OK:
			return reply
		case TimedOut:
			ck.refreshNext()
			time.Sleep(time.Millisecond * 50)
		default:
			ck.refreshNext()
		}
	}
}
func (ck *Clerk) getNext() int {
	return ck.next
}
func (ck *Clerk) setNext(next int) {
	ck.next = next
}
func (ck *Clerk) refreshNext() int {
	ck.next = int(nrand()) % len(ck.servers)
	return ck.next
}
