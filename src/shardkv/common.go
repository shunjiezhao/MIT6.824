package shardkv

import "fmt"

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
	ErrTimeOut     = "ErrTimeOut"
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
	Type  string
	Key   string
	Value string
}

func (o OpArgs) String() string {
	return fmt.Sprintf("ClientID: %v, SeqNum: %v, Type: %v, Key: %v, Value: %v", o.ClientID, o.SeqNum, o.Type, o.Key, o.Value)
}

type OpReply struct {
	Err   Err
	Value string
}

func (o OpReply) String() string {
	return fmt.Sprintf("Err: %v, Value: %v", o.Err, o.Value)
}
