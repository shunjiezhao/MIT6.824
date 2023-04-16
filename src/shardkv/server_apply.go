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
			var reply OpReply
			switch msg.Command.(type) {
			case CMDConfigArgs:
				args := msg.Command.(CMDConfigArgs)
				sc.configProducerC(args.Clone(), &reply)
			case *CMDConfigArgs:
				sc.configProducerC(msg.Command.(*CMDConfigArgs).Clone(), &reply)
			case *CMDMoveShardArgs:
				sc.applyShardDataC(msg.Command.(*CMDMoveShardArgs).Clone(), &reply)
			case CMDMoveShardArgs:
				args := msg.Command.(CMDMoveShardArgs)
				sc.applyShardDataC(args.Clone(), &reply)
			case *AckArgs:
				sc.applyAck(msg.Command.(*AckArgs), &reply)
			case AckArgs:
				args := msg.Command.(AckArgs)
				sc.applyAck(&args, &reply)
			default:
				sc.consumeOP(msg, &reply)
			}
		} else {
			panic("invalid msg")
		}
		if sc.shouldSnapShotL() {
			snapshot := sc.GetSnapshot()
			go sc.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}
}

func (sc *ShardKV) consumeOP(msg raft.ApplyMsg, reply *OpReply) {
	sc.Lock("consumeOP")
	defer sc.UnLock("consumeOP")
	op, ok := msg.Command.(*OpArgs)
	if !ok {
		args := msg.Command.(OpArgs)
		op = &args
	}

	var prt OpReply
	if sc.canServe(op.Shard) {
		Debug(sc, dInfo, "can serve: %+v", op)
		if op.Type == Put || op.Type == Append {
			info := sc.lastExec[op.ClientID]
			if info.SeqNum >= op.SeqNum {
				reply.Err = info.OpResult.Err
				reply.Value = info.OpResult.Value
				return
			}
		}

		if op.Type == Put {
			prt = sc.putL(op)
		} else if op.Type == Append {
			prt = sc.appendL(op)
		} else {
			prt = sc.getL(op)
		}

		reply.Err = prt.Err
		reply.Value = prt.Value
		if op.Type != Get {
			sc.lastExec[op.ClientID] = LastOpInfo{op.SeqNum, prt}
		}
	} else {
		Debug(sc, dInfo, "can not serve: status: %s cfg: %+v op: %s", sc.store[op.Shard].Status, sc.curCfg, op)
		reply.Err = ErrWrongGroup
	}

	ch := sc.ResponseCh[msg.CommandIndex]

	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		return
	}

	go func(reply OpReply) {
		Debug(sc, dChan, "send response: %+v", reply)
		if ch != nil {
			ch <- reply
		}
	}(prt)
}

func (sc *ShardKV) getL(op *OpArgs) OpReply {
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

func (sc *ShardKV) putL(op *OpArgs) OpReply {
	err := sc.store[op.Shard].Put(op.Key, op.Value)
	panicIf(err != nil, "putL: %v", err)
	return OpReply{
		Err: OK,
	}
}

func (sc *ShardKV) appendL(op *OpArgs) OpReply {
	err := sc.store[op.Shard].Append(op.Key, op.Value)
	panicIf(err != nil, "appendL: %v", err)
	return OpReply{
		Err: OK,
	}
}
