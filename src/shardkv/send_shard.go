package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"sync"
)

type MoveShardArgs struct {
	Shard     []int
	ConfigNum int
}

type LastOpInfo struct {
	SeqNum   int64
	OpResult OpReply
}

type MoveShardReply struct {
	Err        Err
	ConfigNum  int
	ClientInfo map[ClientID]LastOpInfo
	ShardData  map[int]store
}

func (r *MoveShardReply) String() string {
	return fmt.Sprintf("Err:%v ClientInfo:%v ShardData:%v", r.Err, r.ClientInfo, r.ShardData)
}

func (r *MoveShardReply) Clone() *MoveShardReply {
	var ans = make(map[int]store, len(r.ShardData))
	for k, v := range r.ShardData {
		ans[k] = v.Clone()
	}

	return &MoveShardReply{
		Err:        r.Err,
		ConfigNum:  r.ConfigNum,
		ClientInfo: copyClientInfo(r.ClientInfo),
		ShardData:  ans,
	}
}

func copyClientInfo(clientInfo map[ClientID]LastOpInfo) map[ClientID]LastOpInfo {
	newClientInfo := make(map[ClientID]LastOpInfo)
	for k, v := range clientInfo {
		newClientInfo[k] = v
	}
	return newClientInfo
}

// 如果发现大于当前的配置，我们不传，如果发现小于，我们传
func (sk *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	sk.Lock("move Shards rpc")
	defer func() {
		sk.UnLock("move Shards rpc")
		Debug(sk, dRpc, "move Shards rpc reply: %s", reply)
	}()

	curNum := sk.curCfg.Num
	println("moveShard", args.ConfigNum, curNum)
	if args.ConfigNum > curNum {
		reply.Err = ErrHigh
		return
	}
	println("moveShard", args.ConfigNum, curNum)
	//	直接传
	// 对方收到之后也不会更新，因为对方的配置大于对方的配置[<]
	reply.Err = OK
	reply.ConfigNum = args.ConfigNum

	reply.ShardData = make(map[int]store)
	for _, shard := range args.Shard {
		reply.ShardData[shard] = sk.store[shard].Store.Clone() // 1
	}

	reply.ClientInfo = copyClientInfo(sk.lastExec) // 2
}

// 获取当前配置下的状态为state的shard
func (sk *ShardKV) getShardByStateL(state ShardStatus) []int {
	ids := make([]int, 0)
	var cnt = 0
	for idx, shard := range sk.store {
		cnt++
		if shard.Status == state {
			ids = append(ids, idx)
		}
	}
	panicIf(cnt != shardctrler.NShards, "shard num not equal")
	return ids
}

func (sk *ShardKV) getShardGrByStateL(state ShardStatus) map[int][]int {
	gr := make(map[int][]int)
	sids := sk.getShardByStateL(state)
	for _, shard := range sids {
		gr[sk.preCfg.Shards[shard]] = append(gr[sk.preCfg.Shards[shard]], shard) //获取上一个配置的地址
	}
	return gr
}

func (sk *ShardKV) ShardStatus() string {
	var str string
	for idx, shard := range sk.store {
		str += fmt.Sprintf("%d:%s\t", idx, shard.Status)
	}
	return str
}

// deamon
func (sk *ShardKV) shardConsumer() {
	sk.Lock("Shards consumer")
	wg := &sync.WaitGroup{}
	gr := sk.getShardGrByStateL(Pulling)
	Debug(sk, dInfo, "get pull Shards map:%+v", gr)
	for gid, shards := range gr {
		Debug(sk, dInfo, "pull Shards1 %v from %v", shards, sk.preCfg.Groups[gid])
		wg.Add(1)

		go func(num int, shard []int, servers []string) {
			defer wg.Done()
			Debug(sk, dInfo, "pull Shards2 %v", shard)
			for si := 0; si < len(servers); si++ {
				println("call22 server", si)
				srv := sk.make_end(servers[si])
				var args = MoveShardArgs{
					Shard:     shard,
					ConfigNum: num,
				}
				var reply MoveShardReply

				ok := srv.Call("ShardKV.MoveShard", &args, &reply)
				Debug(sk, dRpc, "call MoveShard to %v ok: %v args:%+v reply:%+v", servers[si], ok, args, reply)
				if !ok {
					continue
				}

				if reply.Err == OK {
					sk.Exec(&CMDMoveShardArgs{shard, reply.Clone()}, &OpReply{})
				}

				if reply.Err == ErrWrongLeader {
					continue
				}
				if reply.Err == ErrHigh || reply.Err == ErrNotMatchConfigNum {
					break //太高了下轮再试把
				}
			}

		}(sk.curCfg.Num, shards, sk.preCfg.Groups[gid])
	}
	sk.UnLock("Shards consumer")
	wg.Wait()
}

func (sk *ShardKV) applyShardDataC(args *CMDMoveShardArgs, reply *OpReply) {
	moveReply := args.MoveShardReply

	sk.Lock("apply Shards data")
	defer sk.UnLock("apply Shards data")

	if moveReply.ConfigNum != sk.curCfg.Num {
		Debug(sk, dTOut, "config num not match r:%v me:%v", moveReply.ConfigNum, sk.curCfg.Num)
		reply.Err = ErrNotMatchConfigNum
		return
	}

	Debug(sk, dShard, "applyShards data %+v", args)
	reply.Err = OK
	for sid, data := range moveReply.ShardData {
		if sk.store[sid].Status == Pulling {
			for k, v := range data {
				sk.store[sid].Store[k] = v
			}
			sk.store[sid].Status = GC
			Debug(sk, dShard, "change shard %v status to GC", sid)
		} else {
			break //这一轮早已经处理完了
		}
	}

	// 更新clientInfo
	for id, last := range moveReply.ClientInfo {
		if last.SeqNum > sk.lastExec[id].SeqNum { // max
			sk.lastExec[id] = last
		}
	}
}
