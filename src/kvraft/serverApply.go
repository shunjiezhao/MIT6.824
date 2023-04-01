package kvraft

func (kv *KVServer) apply() {
	for msg := range kv.applyCh {
		if msg.CommandValid == false {
			Debug(kv, dWarn, "msg is not valid %s", msg)
			panic("")
		}
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		ch, ok := kv.Response[msg.CommandIndex]
		if (!ok || ch == nil) && isLeader {
			Debug(kv, dWarn, "ch: %v msg: %s", ch, msg) //可能是因为start 后直接宕机了
			//panic("we don't have commit index")
			//这里需要将结果记录然后 然后在插入的时候返回
		}

		op := msg.Command.(Op)
		var (
			value string
			err   error
		)
		switch op.OpType {
		case GET:
			value, err = kv.Store.Get(op.Key)
		case PUT:
			err = kv.Store.Put(op.Key, op.Value)
		case APPEND:
			err = kv.Store.Append(op.Key, op.Value)
		default:
			Debug(kv, dWarn, "OP: %s", op.OpType)
			panic("don't support this op type")

		}

		if isLeader == false {
			kv.mu.Unlock()
			continue
		}

		resp := ApplyResp{
			Response: value,
		}
		if err == nil {
			resp.Status = OK
		} else {
			resp.Status = err.Error()
		}
		// 保存结果
		kv.Result[op.ClientID] = append(kv.Result[op.ClientID], ResultInfo{
			SeqNum: op.SeqNum,
			OpReply: OpReply{
				BaseResp{
					ApplyResp: resp,
				},
			},
		})
		kv.mu.Unlock()
		Debug(kv, dInfo, "send response: %+v", resp)
		ch <- resp
	}

}
