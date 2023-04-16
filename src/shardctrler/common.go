package shardctrler

import "fmt"

//
// Shards controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) getGroupCount() []int {
	var ans []int
	for gid, shards := range c.Groups {
		if len(shards) > 0 {
			ans = append(ans, gid)
		}
	}
	return ans

}

func (c *Config) Clone() Config {
	c2 := Config{}
	c2.Num = c.Num
	c2.Shards = c.Shards
	c2.Groups = copyMap(c.Groups)
	return c2
}

const (
	OK       = "OK"
	TimedOut = "TimedOut"
)

type Err string

type BaseReq struct {
	ClientID ClientID
	SeqNum   int64
	Type     uint8
}

type OpArgs struct {
	Servers map[int][]string // new GID -> servers mappings [Join]
	GIDs    []int            // GIDs to leaveL [Leave]
	Shard   int              // shard to moveL [Move]
	GID     int              // GID to moveL to [Move]
	Num     int              // desired config number [Query]
	BaseReq
}

func (op OpArgs) String() string {
	return fmt.Sprintf("ClientID: %v, SeqNum: %v, Type: %v, Servers: %v, GIDs: %v, Shards: %v, GID: %v, Num: %v", op.ClientID, op.SeqNum, op.Type, op.Servers, op.GIDs, op.Shard, op.GID, op.Num)
}

type OpReply struct {
	Config      Config // config number [Query]
	Err         Err
	WrongLeader bool
}

func (op OpReply) String() string {
	return fmt.Sprintf("Config: %v, Err: %v, WrongLeader: %v", op.Config, op.Err, op.WrongLeader)
}

type MoveArgs struct {
	GID   int // GID to moveL to [Move]
	Shard int // shard to moveL [Move]
	BaseReq
}

type MoveReply struct {
	Err         Err
	WrongLeader bool
}
