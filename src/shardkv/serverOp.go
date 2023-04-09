package shardkv

func (kv *ShardKV) Op(args *OpArgs, reply *OpReply) {
	Debug(kv, dRpc, "Op: %v", args)
}
