package shardkv

import (
	"fmt"
	"time"
)

type MoveShardArgs struct {
	Shard     int
	ConfigNum int
}

type MoveShardReply struct {
	Err        Err
	ConfigNum  int
	ClientInfo map[ClientID]int64
	ShardData  Store
}

func (r *MoveShardReply) String() string {
	return fmt.Sprintf("Err: %v, ConfigNum: %v, ClientInfo: %v, ShardData: %v", r.Err, r.ConfigNum, r.ClientInfo, r.ShardData)
}

func (r *MoveShardReply) Clone() *MoveShardReply {
	return &MoveShardReply{
		Err:        r.Err,
		ConfigNum:  r.ConfigNum,
		ClientInfo: copyClientInfo(r.ClientInfo),
		ShardData:  r.ShardData.Clone(),
	}
}

func copyClientInfo(clientInfo map[ClientID]int64) map[ClientID]int64 {
	newClientInfo := make(map[ClientID]int64)
	for k, v := range clientInfo {
		newClientInfo[k] = v
	}
	return newClientInfo
}
func (sk *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	sk.Lock("move Shards rpc")
	defer func() {
		sk.UnLock("move Shards rpc")
		Debug(sk, dRpc, "move Shards rpc reply: %s", reply)
	}()

	curNum := sk.curCfg.Num
	reply.ConfigNum = curNum
	if args.ConfigNum != curNum {
		Debug(sk, dRpc, "preCfg num not match r:%v me:%v", args.ConfigNum, curNum)
		reply.Err = ErrNotMatchConfigNum
		return
	}

	// configNum match
	panicIf(args.ConfigNum != curNum, fmt.Sprintf("preCfg num not match %v", curNum))
	panicIf(sk.store[args.Shard].Status != Delete, fmt.Sprintf("Shards status not match %v", sk.store[args.Shard].Status))

	//拷贝信息
	reply.Err = OK
	reply.ShardData = sk.store[args.Shard].Clone() // 1
	reply.ClientInfo = copyClientInfo(sk.lastExec) // 2
}

// 获取当前配置下的状态为state的shard
func (sk *ShardKV) getShardByStateL(state ShardStatus) []int {
	ids := make([]int, 0)
	for idx, shard := range sk.store {
		if shard.Status == state {
			ids = append(ids, idx)
		}
	}
	return ids
}
func (sk *ShardKV) ShardStatus() string {
	var str string
	for idx, shard := range sk.store {
		str += fmt.Sprintf("%d:%s\t", idx, shard.Status)
	}
	return str
}

var ShardConsumerInterval = time.Millisecond * 25

// deamon
func (sk *ShardKV) shardConsumer() {
	for {
		time.Sleep(ShardConsumerInterval)
		_, leader := sk.rf.GetState()
		if !leader {
			continue
		}

		// leader 拉取数据
		sk.Lock("Shards consumer")
		pull := sk.getShardByStateL(Pulling)
		if len(pull) == 0 {
			sk.UnLock("Shards consumer")
			continue
		}
		Debug(sk, dInfo, "pull Shards %v", pull)

		for _, shard := range pull {
			gid := sk.preCfg.Shards[shard]
			go func(num, shard int, servers []string) {
				Debug(sk, dInfo, "pull Shards %v", shard)
				for si := 0; si < len(servers); si++ {
					Debug(sk, dRpc, "call MoveShard to %v", servers[si])
					srv := sk.make_end(servers[si])
					var args = MoveShardArgs{
						Shard:     shard,
						ConfigNum: num,
					}
					var reply MoveShardReply
					ok := srv.Call("ShardKV.MoveShard", &args, &reply)
					Debug(sk, dRpc, "call MoveShard args:%+v reply:%+v", args, reply)
					if ok && reply.Err == OK {
						// 应用数据
						// 发送日志
						_, _, isLeader := sk.rf.Start(CMDMoveShardArgs{
							Shards:         shard,
							MoveShardReply: reply.Clone(),
						})
						if isLeader {
							Debug(sk, dInfo, "send pull shard log to raft %d", shard)
						}
					}
					if ok && reply.Err == ErrWrongLeader {
						continue
					}
				}

			}(sk.curCfg.Num, shard, sk.preCfg.Groups[gid])
		}
		sk.UnLock("Shards consumer")
	}
}
func (sk *ShardKV) applyShardDataC(args CMDMoveShardArgs) {
	shard := args.Shards
	reply := args.MoveShardReply
	sk.Lock("apply Shards data")
	defer sk.UnLock("apply Shards data")
	Debug(sk, dInfo, "applyShardDataC args:%s curCfg:%+v", args, sk.curCfg)

	if reply.ConfigNum != sk.curCfg.Num {
		Debug(sk, dTOut, "preCfg num not match r:%v me:%v", reply.ConfigNum, sk.curCfg.Num)
		return
	}

	if sk.store[shard].Status != Pulling { // ????GC,Serveing
		Debug(sk, dTOut, "Shards status not match %v", sk.store[shard].Status)
		return
	}
	// ==
	sk.store[shard] = &Shard{ // pull
		Store:  reply.ShardData,
		Status: GC,
	}
	Debug(sk, dShard, "change shard %d status to %s", shard, sk.store[shard].Status)

	// 更新clientInfo
	for id, last := range reply.ClientInfo {
		if last > sk.lastExec[id] { // max
			sk.lastExec[id] = last
		}
	}
}
