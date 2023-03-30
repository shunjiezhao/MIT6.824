package kvraft

import "time"

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Lock()
	index, _, leader := kv.rf.Start(Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: args.OpType,
	})
	//Debug(kv, dInfo, "args: %s", args)
	println(index, leader)
	if !leader {
		reply.LeaderId = kv.rf.GetLeader()
		reply.Status = ErrNotLeader
		kv.UnLock()
		return
	}
	ch := make(chan ApplyResp)
	Debug(kv, dCH, "add %v chan", index)
	kv.Response[index] = ch
	kv.UnLock()
	select {
	case <-time.After(time.Millisecond * 500):
		Debug(kv, dWarn, "handle req timeout: req: %s", args)
		reply.Status = ErrTimeOut
	case resp := <-ch:
		Debug(kv, dInfo, "receive response : %v", resp)
		reply.ApplyResp = resp
	}

	return
}
