package kvraft

import (
	"fmt"
	"time"
)

func getLocal(string2 string) string {
	return fmt.Sprintf("%s %v", string2, time.Now().Unix())
}
func (kv *KVServer) Op(args *OpArgs, reply *OpReply) {
	local := getLocal("op ")
	kv.Lock(local)
	var (
		index  int
		leader bool
		ch     chan ApplyResp
	)
	if _, ok := kv.Result[args.ClientID]; !ok {
		kv.Result[args.ClientID] = map[int64]OpReply{}
	}

	clientRes := kv.Result[args.ClientID]
	if res, ok := clientRes[args.SeqNum]; ok {
		reply.Status = res.Status
		reply.Response = res.Response
		goto done
	}

	index, _, leader = kv.rf.Start(Op{
		BaseReq: args.BaseReq,
		Key:     args.Key,
		Value:   args.Value,
		OpType:  args.OpType,
	})
	if !leader {
		reply.Status = ErrNotLeader
		Debug(kv, dInfo, "leaderId:%d me:%d", reply.LeaderId, kv.me)
		goto done
	}

	Debug(kv, dTest, "%s index: %d leader: %v", args, index, leader)

	if _, ok := kv.Response[index]; !ok {
		ch = make(chan ApplyResp)
		kv.Response[index] = ch
		Debug(kv, dCH, "add %v chan", index)
	} else {
		Debug(kv, dCH, "exist %v chan", index)
	}
	kv.UnLock(local)
	if ch == nil {
		panic("ch is nil")
	}
	select {
	case <-time.After(time.Millisecond * 500):
		Debug(kv, dWarn, "handle req timeout: req: %s", args)
		reply.Status = ErrTimeOut
	case resp := <-ch:
		Debug(kv, dInfo, "receive response : %v", resp)
		reply.ApplyResp = resp
	}
	Debug(kv, dTest, "%s index: %d leader: %d", reply, index, leader)
	return
done:
	kv.UnLock(local)
}
