package shardctrler

import (
	"6.5840/raft"
	"sort"
)

func (sc *ShardCtrler) apply() {
	for msg := range sc.applyCh {
		Debug(sc, dInfo, "apply msg: %v", msg)
		switch msg.CommandValid {
		case msg.SnapshotValid:
		case msg.CommandValid:
			sc.consumeOP(msg)
		}
	}
}

func (sc *ShardCtrler) consumeOP(msg raft.ApplyMsg) {
	sc.Lock("consumeOP")
	defer sc.UnLock("consumeOP")

	op := msg.Command.(OpArgs)
	ch := make(chan OpReply)
	var reply OpReply

	if sc.lastExec[op.ClientID] < op.SeqNum {
		panicIf(op.SeqNum-sc.lastExec[op.ClientID] > 1, "lastExec: %v now: %d consumeOP: %v", sc.lastExec[op.ClientID], op.Num, op)
		sc.lastExec[op.ClientID] = op.SeqNum
		switch op.Type {
		case Join:
			reply = sc.joinL(op)
		case Leave:
			reply = sc.leaveL(op)
		case Move:
			reply = sc.moveL(op)
		case Query:
			reply = sc.queryL(op)
		}

		ch = sc.resultChan[msg.CommandIndex]
		sc.Result[op.ClientID] = reply
		_, isLeader := sc.rf.GetState()
		if isLeader == false {
			return
		}
	} else {
		reply = sc.Result[op.ClientID]
	}
	go func(reply OpReply) {
		Debug(sc, dInfo, "send response: %+v", reply)
		if ch != nil {
			ch <- reply
		}
	}(reply)
}

func (sc *ShardCtrler) joinL(args OpArgs) OpReply {
	newConfig := sc.getNewConfigL()

	add := make([]int, 0)
	for gid, val := range args.Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			var slice = make([]string, len(val))
			copy(slice, val)
			newConfig.Groups[gid] = slice
			Debug(sc, dInfo, "add group: %v-%v", gid, val)
			add = append(add, gid)
		}
	}
	sc.configs = append(sc.configs, newConfig)
	Debug(sc, dInfo, "joinL-before: %v ", sc.Shards)
	sc.joinBalanceL(add)
	Debug(sc, dInfo, "joinL-balance-done: %v", sc.Shards)
	Debug(sc, dInfo, "joinL done: %v", sc.configs[len(sc.configs)-1])
	var reply OpReply
	reply.WrongLeader = false
	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) getNewConfigL() Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := lastConfig.Clone()
	newConfig.Num++
	return newConfig
}
func (sc *ShardCtrler) leaveL(args OpArgs) OpReply {
	newConfig := sc.getNewConfigL()

	del := make([]int, 0)
	for _, gid := range args.GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid) //删除
			del = append(del, gid)
			// 并将它的分片分配给其他组
		}
	}
	sc.configs = append(sc.configs, newConfig)
	//重新分配
	Debug(sc, dInfo, "Leave-before: %v ", sc.Shards)
	sc.leaveBalanceL(del)
	Debug(sc, dInfo, "Leave-balance-done: %v", sc.Shards)
	Debug(sc, dInfo, "Leave done: %v", sc.configs[len(sc.configs)-1])

	var reply OpReply
	reply.WrongLeader = false
	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) moveL(args OpArgs) OpReply {
	lastConfig := sc.configs[len(sc.configs)-1]
	Debug(sc, dInfo, "moveL-before: %v ", lastConfig)
	newConfig := sc.getNewConfigL()
	prevGID := newConfig.Shards[args.Shard]
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, newConfig)
	var now []int
	for _, shard := range sc.Shards[prevGID] {
		if shard != args.Shard { //将拥有的排除shard
			now = append(now, shard)
		}
	}
	sc.Shards[prevGID] = now
	sc.Shards[args.GID] = append(sc.Shards[args.GID], args.Shard)
	Debug(sc, dInfo, "moveL done: %v", sc.configs[len(sc.configs)-1])

	var reply OpReply
	reply.WrongLeader = false
	reply.Err = OK
	return reply
}

func (sc *ShardCtrler) queryL(args OpArgs) OpReply {
	var resp OpReply
	if args.Num == -1 {
		resp.Config = sc.configs[len(sc.configs)-1]
		resp.Err = OK
		return resp
	}

	idx := sort.Search(len(sc.configs), func(i int) bool {
		return sc.configs[i].Num >= args.Num
	})

	if idx < len(sc.configs) {
		resp.Config = sc.configs[idx]
		resp.Err = OK
		return resp
	}
	resp.Config = sc.configs[idx-1]
	resp.Err = OK
	return resp
}
