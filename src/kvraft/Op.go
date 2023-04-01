package kvraft

import (
	"sort"
	"time"
)

func (kv *KVServer) Op(args *OpArgs, reply *OpReply) {
	kv.Lock()
	var (
		index  int
		leader bool
		ch     chan ApplyResp
	)
	//TODO: 因为更新这里不是一个事务
	// 可能出现更新失败IsExec 失败的

	// 检查是否执行过了
	LastInsert := kv.IsExec[args.ClientID]
	if LastInsert >= args.SeqNum {
		Debug(kv, dInfo, "already exec: %s", args)
		// 将结果返回
		reply.Status = OK
		arr := kv.Result[args.ClientID]
		idx := sort.Search(len(arr), func(i int) bool {
			return arr[i].SeqNum >= args.SeqNum
		})
		if idx == len(arr) || arr[idx].SeqNum != args.SeqNum {
			Debug(kv, dWarn, "%s Result: %v  IsExec: %v", args, kv.Result, kv.IsExec)
			panic("should equal")
		}
		reply.Response = arr[idx].Response
		goto done
	}

	index, _, leader = kv.rf.Start(Op{
		BaseReq: args.BaseReq,
		Key:     args.Key,
		Value:   args.Value,
		OpType:  args.OpType,
	})

	Debug(kv, dTest, "%s index: %d leader: %v", args, index, leader)
	if !leader {
		reply.LeaderId = kv.rf.GetLeader()
		reply.Status = ErrNotLeader
		Debug(kv, dInfo, "leaderId:%d me:%d", reply.LeaderId, kv.me)
		goto done
	}

	kv.IsExec[args.ClientID] = LastInsert // 放在这里
	ch = make(chan ApplyResp)
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
	Debug(kv, dTest, "%s index: %d leader: %d", reply, index, leader)
	return
done:
	kv.UnLock()
}
