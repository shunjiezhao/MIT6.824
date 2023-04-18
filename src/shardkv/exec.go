package shardkv

import (
	"reflect"
	"time"
)

// op
func (kv *ShardKV) Exec(msg any, reply *OpReply) {
	if reflect.ValueOf(msg).Kind() != reflect.Ptr {
		panic("")
	}

	index, _, leader := kv.rf.Start(msg)

	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	Debug(kv, dInfo, "start exec: %v", msg)

	kv.Lock("get ch")
	var ch chan OpReply
	if _, ok := kv.ResponseCh[index]; !ok {
		ch = make(chan OpReply)
		kv.ResponseCh[index] = ch
	} else {
		ch = kv.ResponseCh[index]
	}
	kv.UnLock("get ch")

	panicIf(ch == nil, "ch is nil")
	select {
	case <-time.After(time.Millisecond * 70):
		Debug(kv, dTOut, "handle req timeout: req: %+v", msg)
		reply.Err = ErrTimeOut
	case resp := <-ch:
		Debug(kv, dInfo, "receive response : %v", resp)
		reply.Err = resp.Err
		reply.Value = resp.Value
	}
}
