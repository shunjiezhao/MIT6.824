package kvraft

import "time"

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Lock()
	println("get test1")
	index, _, leader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: GET,
	})
	println(index, leader)
	if !leader {
		println("get test2")
		reply.LeaderId = kv.rf.GetLeader()
		reply.Status = ErrNotLeader
		kv.UnLock()
		return
	}
	println("get test3")
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
