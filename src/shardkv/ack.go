package shardkv

import (
	"sync"
	"time"
)

type AckArgs struct {
	ConfigNum int
	Shard     int
}

type AckReply struct {
	Err       Err
	ConfigNum int
}

func (sk *ShardKV) Ack(args *AckArgs, reply *AckReply) {
	sk.Lock("ack rpc")
	defer func() {
		sk.UnLock("ack rpc")
		Debug(sk, dGC, "ack rpc args: %s reply: %s", args, reply)
	}()

	curNum := sk.curCfg.Num
	reply.ConfigNum = curNum
	reply.Err = OK
	if args.ConfigNum != sk.curCfg.Num { // 更新过了[我们只有当所有的分片都更新的才会替换配置]
		return //过期请求直接返回成功
	}
	// args.configNum == sk.curCfg.Num
	// 更新
	status := sk.store[args.Shard].Status
	panicIf(status != Serveing && status != Delete, "status invalid")
	if status == Serveing {
		return // ok已经更新过了
	}
	_, _, leader := sk.rf.Start(AckArgs{
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	})
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) applyAck(args AckArgs) {
	kv.Lock("applyAck")
	defer kv.UnLock("applyAck")
	// 将 Shards
	if args.ConfigNum != kv.curCfg.Num {
		// 不是本次的
		return
	}

	kv.store[args.Shard].Status = Serveing
	Debug(kv, dGC, "Ack: %v", args.Shard)
}

const (
	GcTimeInterval = time.Millisecond * 25
)

// deamon
func (sk *ShardKV) GCconsumer() {
	for {
		time.Sleep(GcTimeInterval)
		_, leader := sk.rf.GetState()
		if !leader {
			continue
		}

		wg := sync.WaitGroup{}
		sk.Lock("gcing")
		gc := sk.getShardByStateL(GC)
		if len(gc) == 0 {
			sk.UnLock("gcing")
			continue
		}
		Debug(sk, dGC, "gc shards %v", gc)

		for _, shard := range gc {
			gid := sk.preCfg.Shards[shard] //当前配置下分片的group
			go func(num, shard int, servers []string) {
				defer wg.Done()
				wg.Add(1)
				Debug(sk, dGC, "[GC] Shards %v", shard)
				for si := 0; si < len(servers); si++ {
					Debug(sk, dRpc, "call Ack to %v", servers[si])
					srv := sk.make_end(servers[si])
					var args = AckArgs{
						Shard:     shard,
						ConfigNum: num,
					}
					var ok bool
					var reply MoveShardReply
				sendAagin:
					Debug(sk, dRpc, "call Ack %+v", args)
					ok = srv.Call("ShardKV.Ack", &args, &reply)
					if ok && reply.Err == OK {
						sk.rf.Start(CMDAck{
							Shard:     shard,
							ConfigNum: num,
						})
						Debug(sk, dInfo, "send  log to raft %d", shard)
					}
					if ok && reply.Err == ErrSendLog {
						time.Sleep(time.Millisecond * 10)
						goto sendAagin // 下次发送的时候就知道了
					}
					if ok && reply.Err == ErrWrongLeader {
						continue
					}
				}

			}(sk.curCfg.Num, shard, sk.preCfg.Groups[gid])
		}
		sk.UnLock("gcing")

	}
}
