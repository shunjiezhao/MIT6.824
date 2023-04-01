package kvraft

type RegisterReply struct {
	Status     string
	ClientID   int
	LeaderHint int
	ServerId   int
}
type RegisterArgs struct{}

func (kv *KVServer) RegisterClient(args *RegisterArgs, reply *RegisterReply) {
	kv.Lock()
	defer kv.UnLock()
	//TODO:
	reply.ServerId = kv.me
	reply.LeaderHint = kv.rf.GetLeader()
	reply.Status = OK

}
