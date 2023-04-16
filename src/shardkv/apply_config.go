package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"time"
)

const (
	PollInterval           = time.Millisecond * 25
	ConfigConsumerInterval = time.Millisecond * 25
)

// deamon
func (sk *ShardKV) ApplyConfig() {
	for {
		time.Sleep(PollInterval)
		_, leader := sk.rf.GetState()
		if !leader {
			continue
		}
		sk.Lock("check pull Shards")
		if len(sk.getShardByStateL(Serveing)) != shardctrler.NShards {
			Debug(sk, dInfo, "check pull Shards %+v", sk.getShardByStateL(Delete))
			Debug(sk, dInfo, "now: %+v pre: %+v", sk.curCfg, sk.preCfg)
			sk.UnLock("check pull Shards")
			continue
		}
		sk.UnLock("check pull Shards")
		Debug(sk, dConfig, "try config num %+v\n", sk.curCfg.Num+1)
		newConfig := sk.sm.Query(sk.curCfg.Num + 1)
		Debug(sk, dInfo, "pull config  %+v\n ", newConfig)
		sk.Lock("check pull preCfg num")
		if newConfig.Num == sk.curCfg.Num+1 {
			cfg := newConfig.Clone()
			args := CMDConfigArgs{cfg}
			index, term, leader := sk.rf.Start(args)
			if leader {
				Debug(sk, dApply, "apply new config %v index:%v term:%v leader:%v", cfg, index, term, leader)
			}
		}
		sk.UnLock("check pull preCfg num")
	}
	Debug(sk, dErr, "apply done")
}

func (sc *ShardKV) configProducerC(cfg CMDConfigArgs) {
	sc.Lock("apply preCfg")
	defer sc.UnLock("apply preCfg")

	Debug(sc, dInfo, "config producer cur: %+v cfg:%+v", sc.curCfg, cfg)

	if sc.curCfg.Num+1 != cfg.Config.Num {
		Debug(sc, dTOut, "正在拉取配置，不需要更新")
		return
	}

	//panicIf(cfg.Config.Num != sc.preCfg.Num+1, fmt.Sprintf("preCfg num not match %v", sc.curCfg.Num)) // preCfg = curCfg
	// 更新拉取配置信息，以及自己不服务的分片信息
	Debug(sc, dConfig, "update pull config:%+v[%d] num:{%s}", cfg.Config, sc.curCfg.Num, cfg.Config.Num)

	sc.preCfg = sc.curCfg // swap
	sc.curCfg = cfg.Config

	panicIf(sc.curCfg.Num != sc.preCfg.Num+1, fmt.Sprintf("config num not match pull:%v now:%v", sc.curCfg.Num, sc.preCfg.Num))
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
