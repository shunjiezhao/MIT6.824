package shardkv

import (
	"fmt"
	"time"
)

func (kv *ShardKV) Op(args *OpArgs, reply *OpReply) {

	if args == nil || reply == nil {
		panic("args or reply is nil")
	}

	var (
		index  int
		leader bool
		ch     chan OpReply
	)
	cpArgs := *args

	Debug(kv, dRpc, "receive req: %s", args)
	kv.Lock("Op")
	fmt.Println(0)
	if args.Shard < 0 || kv.lastExec[args.ClientID] >= args.SeqNum { // 执行过了
		Debug(kv, dErr, "me: %d wrong group: %v", kv.gid, args)
		reply.Err = OK
		goto done
	}
	fmt.Println(1)

	index, _, leader = kv.rf.Start(cpArgs)
	fmt.Println(2)

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
	return
done:
	kv.UnLock("Op")
}
