package shardkv

import "time"

func (sk *ShardKV) Deamon(do func(), shouldGO bool) {
	for sk.killed() == false {
		time.Sleep(50 * time.Millisecond)
		_, leader := sk.rf.GetState()
		if !leader {
			continue
		}
		do()
	}

}
