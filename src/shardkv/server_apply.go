package shardkv

import (
	"6.5840/raft"
)

// deamon
func (sc *ShardKV) apply() {
	for msg := range sc.applyCh {
		Debug(sc, dApply, "apply msg: %v", msg)
		if msg.SnapshotValid == true {
			sc.InstallSnapshot(msg.Snapshot)
		} else if msg.CommandValid == true {
			switch msg.Command.(type) {
			case CMDConfigArgs:
				sc.configProducerC(msg.Command.(CMDConfigArgs).Clone())
			case CMDMoveShardArgs:
				sc.applyShardDataC(msg.Command.(CMDMoveShardArgs).Clone())
			case CMDAck:
				sc.applyAckCmd(msg.Command.(CMDAck))
			case AckArgs:
				sc.applyAck(msg.Command.(AckArgs))

			default:
				sc.consumeOP(msg)
			}
		} else {
			panic("invalid msg")
		}
	}
}

func (sc *ShardKV) applyAckCmd(args CMDAck) {
	sc.Lock("apply ack")
	defer sc.UnLock("apply ack")
	Debug(sc, dInfo, "apply ack: %+v", args)
	// 将 Shards
	if args.ConfigNum != sc.curCfg.Num {
		// 不是本次的
		return
	}
	// 是本次的
	status := sc.store[args.Shard].Status
	panicIf(status != Serveing && status != GC, "status is not valid in this preCfg")
	sc.store[args.Shard].Status = Serveing
	Debug(sc, dShard, "change Shards status->%s", Serveing)
}

func (sc *ShardKV) consumeOP(msg raft.ApplyMsg) {
	sc.Lock("consumeOP")
	defer sc.UnLock("consumeOP")

	op := msg.Command.(OpArgs)
	if sc.lastExec[op.ClientID] >= op.SeqNum {
		Debug(sc, dInfo, "already executed: %+v", op)
		return
	}

	var reply OpReply
	if sc.canServe(op.Shard) {
		Debug(sc, dInfo, "can serve: %+v", op)
		switch op.Type {
		case Get:
			reply = sc.getL(op)
		case Put:
			reply = sc.putL(op)
		case Append:
			reply = sc.appendL(op)
		}
		sc.lastExec[op.ClientID] = op.SeqNum
	} else {
		Debug(sc, dInfo, "can not serve: status: %s cfg: %+v op: %s", sc.store[op.Shard].Status, sc.curCfg, op)
		reply.Err = ErrWrongGroup
	}

	ch := sc.ResponseCh[msg.CommandIndex]
	if sc.shouldSnapShotL() {
		sc.rf.Snapshot(msg.CommandIndex, sc.GetSnapshot())
	}
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		return
	}

	go func(reply OpReply) {
		Debug(sc, dChan, "send response: %+v", reply)
		if ch != nil {
			ch <- reply
		}
	}(reply)
}

func (sc *ShardKV) getL(op OpArgs) OpReply {
	value, err := sc.store[op.Shard].Get(op.Key)
	reply := OpReply{
		Value: value,
	}
	if err != nil {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
	return reply
}

func (sc *ShardKV) putL(op OpArgs) OpReply {
	err := sc.store[op.Shard].Put(op.Key, op.Value)
	panicIf(err != nil, "putL: %v", err)
	return OpReply{
		Err: OK,
	}
}

func (sc *ShardKV) appendL(op OpArgs) OpReply {
	err := sc.store[op.Shard].Append(op.Key, op.Value)
	panicIf(err != nil, "appendL: %v", err)
	return OpReply{
		Err: OK,
	}
}
