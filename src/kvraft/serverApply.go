package kvraft

import "6.5840/raft"

func (kv *KVServer) apply() {
	for {
		select {
		case msg := <-kv.applyCh:
			if kv.killed() {
				return
			}
			local := getLocal("apply")
			kv.Lock(local)
			if msg.SnapshotValid == true {
				kv.applySnapShotL(msg)
			} else if msg.CommandValid == true {
				kv.applyOpL(msg)
			} else {
				panic("")
			}
			kv.UnLock(local)
		}
	}
}
func (kv *KVServer) applyOpL(msg raft.ApplyMsg) {

	ch := kv.Response[msg.CommandIndex]

	op := msg.Command.(Op)
	lastExec := kv.IsExec[op.ClientID]

	var resp ApplyResp
	if op.SeqNum > lastExec {
		if op.SeqNum != lastExec+1 {
			Debug(kv, dWarn, "op: %s lastExec: %d", op, lastExec)
			panic("should equal")
		}
		resp = kv.storeExecL(op)
		kv.saveToResultL(op, resp)
	} else {
		resp = kv.Result[op.ClientID][op.SeqNum].ApplyResp
	}
	if kv.shouldSnapShotL() {
		kv.rf.Snapshot(msg.CommandIndex, kv.GetStoreBytes())
	}

	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		return
	}
	go func() {
		Debug(kv, dInfo, "send response: %+v", resp)
		if ch != nil {
			ch <- resp
		}
	}()
}

func (kv *KVServer) storeExecL(op Op) ApplyResp {
	var (
		value string
		err   error
	)
	switch op.OpType {
	case GET:
		value, err = kv.Store.Get(op.Key)
	case PUT:
		err = kv.Store.Put(op.Key, op.Value)
	case APPEND:
		err = kv.Store.Append(op.Key, op.Value)
	default:
		Debug(kv, dWarn, "OP: %s", op.OpType)
		panic("don't support this op type")
	}
	kv.IsExec[op.ClientID] = op.SeqNum
	resp := ApplyResp{
		Response: value,
	}
	if err == nil {
		resp.Status = OK
	} else {
		resp.Status = err.Error()
	}
	return resp
}
func (kv *KVServer) saveToResultL(op Op, resp ApplyResp) {
	if _, ok := kv.Result[op.ClientID]; !ok {
		kv.Result[op.ClientID] = map[int64]OpReply{}
	}

	// 保存结果
	kv.Result[op.ClientID][op.SeqNum] = OpReply{
		BaseResp{
			ApplyResp: resp,
		},
	}
	Debug(kv, dInfo, "save reponse op: %s resp: %s", op, resp)
}
func (kv *KVServer) applySnapShotL(msg raft.ApplyMsg) {
	kv.InstallStoreBytes(msg.Snapshot)
}
