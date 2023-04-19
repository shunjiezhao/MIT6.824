package shardkv

import (
	"sync"
)

// GC /pull
type AckArgs struct {
	ConfigNum int
	Shard     []int
}

// 对方并不需要知道配置号，
// 返回成功，对方那边有自己的配置号
func (sk *ShardKV) Ack(args *AckArgs, reply *OpReply) {
	_, leader := sk.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	sk.Lock("ack rpc")
	defer func() {
		Debug(sk, dGC, "ack rpc args: %+v reply: %+v", args, reply)
	}()

	if args.ConfigNum < sk.curCfg.Num { // 更新过了[我们只有当所有的分片都更新的才会替换配置]
		reply.Err = OK
		sk.UnLock("ack rpc")
		return //过期请求直接返回成功
	}
	// args.configNum >= sk.curCfg.Num
	// 更新
	sk.UnLock("ack rpc")
	var opRe OpReply
	sk.Exec(args, &opRe)
	reply.Err = opRe.Err
}

func (kv *ShardKV) applyAck(args *AckArgs, reply *OpReply) {
	kv.Lock("applyAck")
	defer kv.UnLock("applyAck")
	// 将 Shards
	reply.Err = OK

	if args.ConfigNum < kv.curCfg.Num {
		// 不是本次的
		return // 1.是过去的，我们早以删除
		// 2. 是未来的，我们不需要回应。。。。
	}
	if args.ConfigNum > kv.curCfg.Num {
		reply.Err = ErrHigh
		return
	}
	for _, sid := range args.Shard {
		if kv.store[sid].Status == GC {
			kv.store[sid].Status = Serveing // 已经告知对方并且收到ok
		} else if kv.store[sid].Status == Delete {
			// 对方告知我们删除，我们已经回复确认
			kv.store[sid] = NewShard()
		} else {
			// serving
			break //重复更新了
		}
	}
}

// deamon
func (sk *ShardKV) GCconsumer() {
	wg := &sync.WaitGroup{}
	sk.Lock("gcing")
	gc := sk.getShardGrByStateL(GC)
	sk.UnLock("gcing")
	if len(gc) == 0 {
		return
	}

	for gid, sids := range gc {
		wg.Add(1)
		sk.gcHelper(wg, sk.curCfg.Num, sids, sk.preCfg.Groups[gid])
	}
	wg.Wait()
}

func (sk *ShardKV) gcHelper(wg *sync.WaitGroup, num int, shard []int, servers []string) {
	sk.Do(func() {
		defer wg.Done()
		Debug(sk, dGC, "[GC] Shards %v", shard)
		for si := 0; si < len(servers); si++ {
			srv := sk.make_end(servers[si])
			var args = AckArgs{
				Shard:     shard,
				ConfigNum: num,
			}
			var ok bool
			var reply OpReply
			ok = srv.Call("ShardKV.Ack", &args, &reply)
			Debug(sk, dRpc, "call Ack ok:%v args: %+v reply: %s ", ok, args, reply)
			if ok && reply.Err == OK {
				sk.Exec(&args, &OpReply{})
				return
			}
			if reply.Err == ErrTimeOut {
				break
			}
		}
	})

}
