package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)
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
	return w != WorkTNODefine
}
func (id WorkerID) server() {
	log.Printf("工人：%v 正在监听♥检测", id)

	rpc.Register(&id)
	rpc.HandleHTTP()
	sockname := workerSock(id)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (id WorkerID) Ping(req *PingReq, resp PingResp) error {
	if req.Content != "Ping" {
		//TODO:可能主节点挂了，或者结束了
		return fmt.Errorf("不是Ping")
	}
	resp.Content = "Pong"
	return nil
}

const (
	WorkTNODefine WorkerType = iota // free worker
	MapW                            // map worker
	ReduceW                         // reduce worker
)

type WorkerStatus uint8

const (
	WorkerFree WorkerStatus = iota
	Working
	Done
	Dead
)

type Empty struct{}
type RegReq struct{}

// 通知 reduce map 工作结束
type ReduceRevReq struct {
	Files []string //文件名
}

// 注册返回工人编号
type RegResp struct {
	Id WorkerID
}

// 询问请求
type PingReq struct {
	Id      WorkerID //这是注册是返回的工人ID
	Content string
}

// 询问响应
type PingResp struct {
	// 如果type == 0 说明任务已满
	Type    WorkerType // type
	Id      WorkerID   // 工人 Id
	Content string
}

// 询问请求
type AskReq struct {
	Id WorkerID
}

// 询问响应
type AskResp struct {
	// 如果type == 0 说明任务已满
	Type           WorkerType // type
	Id             GroupID    // 组内编号
	InputFileName  string     // map input
	TmpfileName    string     // map 的临时文件名
	OutPutFileName string     //reduce output
	MapCnt         int
	ReduceCnt      int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type CommitReq struct {
	Id    WorkerID
	GID   GroupID
	Type  WorkerType
	Files []string
}

type CommitResp struct{}

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
func workerSock(id WorkerID) string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(int(id))
	return s
}

type GroupID int

func mapWorkerSock(id GroupID) string {
	s := "/var/tmp/824-mr-map-"
	s += strconv.Itoa(int(id))
	return s
}

func reduceWorkerSock(id GroupID) string {
	s := "/var/tmp/824-mr-reduce-"
	s += strconv.Itoa(int(id))
	return s
}
