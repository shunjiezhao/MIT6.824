package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

type Err string
type ClientID int64
type BaseReq struct {
	ClientID ClientID
	SeqNum   int64
}

type OpArgs struct {
	BaseReq
	Op    string
	Key   string
	Value string
}

type OpReply struct {
	Err   Err
	Value string
}
