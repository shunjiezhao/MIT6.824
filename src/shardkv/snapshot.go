package shardkv

import (
	"6.5840/labgob"
	"bytes"
)

func (kv *ShardKV) shouldSnapShotL() bool {
	if kv.persister.RaftStateSize() < kv.maxraftstate-100 || kv.maxraftstate == -1 {
		return false
	}
	return true
}

func (kv *ShardKV) GetSnapshot() []byte {
	kv.Lock("snapshot")
	defer kv.UnLock("snapshot")

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.preCfg) != nil ||
		e.Encode(kv.curCfg) != nil ||
		e.Encode(kv.store) != nil ||
		e.Encode(kv.lastExec) != nil {
		panic("encode error")
	}
	return w.Bytes()
}

func (kv *ShardKV) InstallSnapshot(b []byte) {
	kv.Lock("snapshot")
	defer kv.UnLock("snapshot")
	if len(b) == 0 {
		return
	}
	r := bytes.NewBuffer(b)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.preCfg) != nil ||
		d.Decode(&kv.curCfg) != nil ||
		d.Decode(&kv.store) != nil ||
		d.Decode(&kv.lastExec) != nil {
		panic("decode error")
	}
}
