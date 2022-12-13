package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type WorkerID int64

type WorkerType uint8

func (w WorkerType) String() string {
	switch w {
	case MapW:
		return "Map"
	case ReduceW:
		return "Reduce"
	}
	return ""
}

// 是否分配工作
func (w WorkerType) CanWork() bool {
	return w != 0
}

const (
	MapW    = iota + 1 // map worker
	ReduceW            // reduce worker
)

type Empty struct{}
type RegReq struct{}

// 注册返回工人编号
type RegResp struct {
	Id WorkerID
}

// 询问请求
type PingReq struct {
	Id WorkerID //这是注册是返回的工人ID
}

// 询问响应
type PingResp struct {
	// 如果type == 0 说明任务已满
	Type WorkerType // type
	Id   WorkerID   // 工人 Id
}

// 询问请求
type AskReq struct {
	Id WorkerID
}

// 询问响应
type AskResp struct {
	// 如果type == 0 说明任务已满
	Type WorkerType // type
	Id   int        // 组内编号
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
