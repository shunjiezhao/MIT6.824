package shardctrler

import "sort"

func (sc *ShardCtrler) getExistShardSortKeyL() []int {
	var ans []int
	for gid, _ := range sc.Shards {
		ans = append(ans, gid)
	}
	sort.Ints(ans)
	return ans

}
func (sc *ShardCtrler) joinBalanceL(add []int) {
	if len(add) == 0 {
		return
	}
	defer sc.applyToConfigL()
	pre := sc.getExistShardSortKeyL()
	if len(pre) == 0 {
		sc.Shards = make(map[int][]int)
		sort.Ints(add)
		for i := 0; i < NShards; {
			for _, gid := range add {
				sc.Shards[gid] = append(sc.Shards[gid], i)
				i++
				if i == NShards {
					break
				}
			}
		}
		return
	}

	// 将加入的
	for _, gid := range add {
		if _, ok := sc.Shards[gid]; !ok {
			sc.Shards[gid] = make([]int, 0)
		}
	}

}
func (sc *ShardCtrler) getMaxShardCountGIDL() int {
	var mx int
	var id int
	count := sc.getExistShardSortKeyL()
	for _, gid := range count {
		shards := sc.Shards[gid]
		if len(shards) >= mx {
			mx = len(shards)
			id = max(gid, id)
		}
	}

	return id
}
func (sc *ShardCtrler) getMinShardCountGIDL() int {
	var mn int = NShards + 1
	var id int
	count := sc.getExistShardSortKeyL()
	for _, gid := range count {
		shards := sc.Shards[gid]
		if len(shards) <= mn {
			mn = len(shards)
			id = max(gid, id)
		}
	}
	return id
}

func (sc *ShardCtrler) leaveBalanceL(del []int) {
	if len(del) == 0 {
		return
	}
	defer sc.applyToConfigL()
	take := make([]int, 0)
	for _, gid := range del {
		if shard, ok := sc.Shards[gid]; ok {
			take = append(take, shard...)
			delete(sc.Shards, gid)
		}
	}
	re := sc.getExistShardSortKeyL()
	if len(sc.Shards) == 0 {
		sc.Shards = make(map[int][]int)
		return
	}
	//将所有的shard分配到剩下的group的其中一个中
	sc.Shards[re[0]] = append(sc.Shards[re[0]], take...)
}

func (sc *ShardCtrler) applyToConfigL() {
	for {
		big, small := sc.getMaxShardCountGIDL(), sc.getMinShardCountGIDL()
		if big == 0 {
			break
		}
		if len(sc.Shards[big])-len(sc.Shards[small]) <= 1 {
			break
		}
		sc.Shards[small] = append(sc.Shards[small], sc.Shards[big][0])
		sc.Shards[big] = sc.Shards[big][1:]
	}

	sc.configs[len(sc.configs)-1].Shards = [NShards]int{}
	for gid, shards := range sc.Shards {
		for _, shard := range shards {
			sc.configs[len(sc.configs)-1].Shards[shard] = gid
		}
	}
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b

}
