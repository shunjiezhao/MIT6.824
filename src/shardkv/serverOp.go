package shardkv

import "time"

func (kv *ShardKV) Op(args *OpArgs, reply *OpReply) {
	if args == nil || reply == nil {
		panic("args or reply is nil")
	}

	Debug(kv, dRpc, "Type: %v", args)
	kv.Lock("Op")

	var (
		index  int
		leader bool
		ch     chan OpReply
	)
	cpArgs := *args
	if res, ok := kv.SingleExec.GetIfExec(cpArgs); ok {
		reply.Err = res.(OpReply).Err
		reply.Value = res.(OpReply).Value
		goto done
	}

	index, _, leader = kv.rf.Start(cpArgs)
	if !leader {
		reply.Err = ErrWrongLeader
		goto done
	}

	Debug(kv, dInfo, "%s index: %d leader: %v", args, index, leader)

	if _, ok := kv.ResponseCh[index]; !ok {
		ch = make(chan OpReply)
		kv.ResponseCh[index] = ch
		Debug(kv, dChan, "add %v chan", index)
	} else {
		Debug(kv, dChan, "exist %v chan", index)
		ch = kv.ResponseCh[index]
	}
	kv.UnLock("Op")
	if ch == nil {
		panic("ch is nil")
	}
	select {
	case <-time.After(time.Millisecond * 500):
		Debug(kv, dTOut, "handle req timeout: req: %s", args)
		reply.Err = ErrTimeOut
	case resp := <-ch:
		Debug(kv, dInfo, "receive response : %v", resp)
		reply.Err = resp.Err
		reply.Value = resp.Value
	}
	Debug(kv, dResp, "%s index: %v leader: %v", reply, index, leader)
	return
done:
	kv.UnLock("Op")
}
