package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each Shards.
// Shardctrler may change Shards assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                   = "OK"
	ErrNoKey             = "ErrNoKey"
	ErrWrongGroup        = "ErrWrongGroup"
	ErrWrongLeader       = "ErrWrongLeader"
	ErrTimeOut           = "ErrTimeOut"
	ErrNotMatchConfigNum = "ErrNotMatchConfigNum"
	ErrSendLog           = "ErrSendLog"
)

const (
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
	Config = "Config"
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
	Shard int
}

func (o OpArgs) String() string {
	return fmt.Sprintf("BaseReq: %v, Type: %v, Key: %v, Value: %v, Shards: %v", o.BaseReq, o.Type, o.Key, o.Value, o.Shard)
}

type OpReply struct {
	Err   Err
	Value string
}

func (o OpReply) String() string {
	return fmt.Sprintf("Err: %v, Value: %v", o.Err, o.Value)
}

type CMDConfigArgs struct {
	Config shardctrler.Config
}

func (x CMDConfigArgs) Clone() CMDConfigArgs {
	return CMDConfigArgs{
		Config: x.Config.Clone(),
	}
}

type CMDMoveShardArgs struct {
	Shards int
	*MoveShardReply
}

func (x CMDMoveShardArgs) Clone() CMDMoveShardArgs {
	return CMDMoveShardArgs{
		Shards:         x.Shards,
		MoveShardReply: x.MoveShardReply.Clone(),
	}
}

type CMDAck struct {
	Shard     int
	ConfigNum int
}
