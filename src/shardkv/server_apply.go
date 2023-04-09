package shardkv

import (
	"6.5840/raft"
)

func (sc *ShardKV) apply() {
	for msg := range sc.applyCh {
		Debug(sc, dApply, "apply msg: %v", msg)
		switch msg.CommandValid {
		case msg.SnapshotValid:
		case msg.CommandValid:
			sc.consumeOP(msg)
		}
	}
}

func (sc *ShardKV) consumeOP(msg raft.ApplyMsg) {
	sc.Lock("consumeOP")
	defer sc.UnLock("consumeOP")

	op := msg.Command.(OpArgs)
	if sc.IxExec(msg.Command) {
		return
	}

	var reply OpReply
	sc.Exec(msg.Command, func() any {
		switch op.Type {
		case Get:
			reply = sc.getL(op)
		case Put:
			reply = sc.putL(op)
		case Append:
			reply = sc.appendL(op)
		}
		return reply
	})

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
	}(reply)
}

func (sc *ShardKV) getL(op OpArgs) OpReply {
	value, err := sc.Get(op.Key)
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
	err := sc.Put(op.Key, op.Value)
	panicIf(err != nil, "putL: %v", err)
	return OpReply{
		Err: OK,
	}
}

func (sc *ShardKV) appendL(op OpArgs) OpReply {
	err := sc.Append(op.Key, op.Value)
	panicIf(err != nil, "appendL: %v", err)
	return OpReply{
		Err: OK,
	}
}
