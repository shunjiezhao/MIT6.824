package shardkv

func (kv *ShardKV) Op(args *OpArgs, reply *OpReply) {
	_, leader := kv.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Lock("Op")
	if args.Type != Get && kv.isDuplicateRequest(args.ClientID, args.SeqNum) {
		lastResponse := kv.lastExec[args.ClientID].OpResult
		reply.Err, reply.Value = lastResponse.Err, lastResponse.Value
		kv.UnLock("Op")
		return
	}

	//  can server?
	if !kv.canServe(args.Shard) {
		reply.Err = ErrWrongGroup
		kv.UnLock("Op")
		return
	}
	kv.UnLock("Op")

	kv.Exec(args, reply)
}
