package shardkv

import (
	"6.5840/shardctrler"
)

// deamon
func (sk *ShardKV) PullConfig() {
	sk.Lock("check pull Shards")
	nextNum := sk.curCfg.Num + 1

	if len(sk.getShardByStateL(Serveing)) != shardctrler.NShards {
		Debug(sk, dInfo, "now: %+v \npre: %+v \n status: %s\n", sk.curCfg, sk.preCfg, sk.ShardStatus())
		sk.UnLock("check pull Shards")
		return
	}
	sk.UnLock("check pull Shards")

	newConfig := sk.sm.Query(nextNum)
	Debug(sk, dInfo, "pull config num:%d %+v\n\n", nextNum, newConfig)
	if newConfig.Num == nextNum {
		args := &CMDConfigArgs{
			Config: newConfig,
		}
		sk.Exec(args, &OpReply{})
	}
}

func (sc *ShardKV) configProducerC(cfg *CMDConfigArgs, reply *OpReply) {
	sc.Lock("apply preCfg")
	defer sc.UnLock("apply preCfg")

	Debug(sc, dInfo, "config producer cur: %+v cfg:%+v", sc.curCfg, cfg)

	if sc.curCfg.Num+1 != cfg.Config.Num {
		reply.Err = ErrNotMatchConfigNum
		return
	}

	//panicIf(cfg.Config.Num != sc.preCfg.Num+1, fmt.Sprintf("preCfg num not match %v", sc.curCfg.Num)) // preCfg = curCfg
	// 更新拉取配置信息，以及自己不服务的分片信息
	Debug(sc, dConfig, "update pull config:%+v[%d] num:{%v}", cfg.Config, sc.curCfg.Num, cfg.Config.Num)

	sc.preCfg = sc.curCfg // swap
	sc.curCfg = cfg.Config

	reply.Err = OK
	if sc.curCfg.Num != 1 {
		var nowMes = make([]int, 0)
		var preMes = make([]int, 0)
		// 原来拥有的分片
		// 更新自己拥有分片信息的state
		for shard, gid := range sc.preCfg.Shards {
			if gid == sc.gid {
				preMes = append(preMes, shard)
			}
		}

		for shard, gid := range sc.curCfg.Shards {
			if gid == sc.gid {
				nowMes = append(nowMes, shard)
			}
		}

		i, j := 0, 0
		for i < len(preMes) && j < len(nowMes) {
			if preMes[i] == nowMes[j] {
				i++
				j++
			} else if preMes[i] < nowMes[j] {
				sc.store[preMes[i]].Status = Delete // 现在没有的
				i++
			} else {
				sc.store[nowMes[j]].Status = Pulling // 现在有的,愿来没有
				j++
			}
		}

		for i < len(preMes) {
			sc.store[preMes[i]].Status = Delete // 现在没有的
			i++
		}

		for j < len(nowMes) {
			sc.store[nowMes[j]].Status = Pulling // 现在有的,愿来没有
			j++
		}
	}
	Debug(sc, dConfig, "update Shards status:{%s}", sc.ShardStatus())
	//状态更新完毕
	// 等待分片消费者拉取自己的分片
}
