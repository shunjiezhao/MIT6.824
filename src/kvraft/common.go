package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotLeader   = "I'm not leader"
	ErrTimeOut     = "time out"
	GET            = "Get"

	PUT = "Put"

	APPEND = "Append"
)

type (
	Err     string
	BaseReq struct {
		ClientID int
		SeqNum   int64
	}
	ApplyResp struct {
		Status   string
		Response string
	}
	BaseResp struct {
		ApplyResp
		LeaderId int
	}
)

func (r BaseReq) String() string {
	return fmt.Sprintf("BaseReq:{clientID: %d SeqNum: %v}", r.ClientID, r.SeqNum)
}
func (r BaseResp) String() string {
	return fmt.Sprintf("status: %v response: %v leaderHint: %v", r.Status, r.Response, r.LeaderId)
}

//type GetArgs struct {
//	BaseReq
//	Key string
//	ClientInfo
//	// You'll have to add definitions here.
//}
//
//func (r GetArgs) String() string {
//	return fmt.Sprintf("GetArgs: %s key: %v", r.BaseReq, r.Key)
//}
//
//type GetReply struct {
//	BaseResp
//}
//
//func (r GetReply) String() string {
//	return fmt.Sprintf("GetReply: status: %v response: %v leaderHint: %v", r.Status, r.Response, r.LeaderId)
//}

type OpArgs struct {
	BaseReq
	Key, Value string
	OpType     string
}

func (r OpArgs) String() string {
	return fmt.Sprintf("OpArgs:  %s  Type: %s key: %v value: %v", r.BaseReq, r.OpType, r.Key, r.Value)
}

type OpReply struct {
	BaseResp
}

func (r OpReply) String() string {
	return fmt.Sprintf("OpReply: status: %v response: %v leaderHint: %v", r.Status, r.Response, r.LeaderId)
}
