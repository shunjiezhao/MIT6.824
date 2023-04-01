package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type ClientInfo struct {
	SeqId, ClientId int64
}

func (c ClientInfo) String() string {
	return fmt.Sprintf("ClientId: %v SeqId: %v", c.ClientId, c.SeqId)
}

// TODO: server ID not equal server
// inuse map
// need to register
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	idx      map[int]int
	leaderId int
	mu       *sync.Mutex
	ID       int
	SeqNum   atomic.Int64
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
	ck.leaderId = -1
	ck.idx = map[int]int{}
	ck.ID = int(nrand())
	ck.mu = &sync.Mutex{}
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Op(key, "", GET)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Op", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Op(key string, value string, op string) string {
	defer func() {
		println(op, "success")
	}()
	var req = OpArgs{
		BaseReq: BaseReq{
			ClientID: ck.ID,
			SeqNum:   ck.SeqNum.Add(int64(1)),
		},
		Key:    key,
		Value:  value,
		OpType: op,
	}

	Debug(nil, dClient, "%s begin %s", op, req)
	var reply OpReply

	next := ck.leaderId
	if next == -1 {
		next = int(nrand()) % len(ck.servers)
	}

	for {

		Debug(nil, dClient, "call %d server", next)

		var call bool
		call = ck.servers[next].Call("KVServer.Op", &req, &reply)
		// You will have to modify this function.
		if !call {
			continue
		}

		Debug(nil, dInfo, "%d call reply %s", next, reply)
		switch reply.Status {
		case OK:
			ck.leaderId = next
			return reply.Response

		case ErrNotLeader:
			//ck.leaderId = reply.LeaderId
			Debug(nil, dClient, "get leaderid: %v", ck.leaderId)
			//if ck.leaderId == -1 {
			next = int(nrand()) % len(ck.servers)
			//}
			continue

		case ErrNoKey:
			return ""
		case ErrTimeOut:
			Debug(nil, dClient, "query time out")
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.Op(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Op(key, value, APPEND)
}
