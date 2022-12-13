package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	workerID WorkerID // 分配给每个工人
	wMap     sync.RWMutex
	workers  map[WorkerID]WorkerType // 记录活跃的工人
	// 维护状态 然后需要
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Printf("receive %v", args.X)
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Ask(args *AskReq, reply *AskResp) error {
	log.Printf("receive worker:%v ask\n", args.Id)
	// 检查是否有任务可以分配
	reply.Type = c.CheckCanAlloc()
	if reply.Type == 0 {
		log.Println("all tasks are done by workers")
		return nil
	}
	log.Printf("alloc %v task to worker:%s\n", reply.Type.String(), args.Id)
	//TODO：将分配的任务注册并分配 各个任务（map or reduce）的组内id
	// Map 还需返回输入文件名
	// reduce 需要返回输出文件名嘛

	return nil
}

func (c *Coordinator) Register(args *RegReq, reply *RegResp) error {
	//TODO:注册需要传递参数嘛
	//log.Printf("receive %v")
	c.wMap.Lock()
	defer c.wMap.Unlock()
	c.workerID++
	reply.Id = c.workerID
	c.workers[reply.Id] = 0
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) CheckCanAlloc() WorkerType {
	return 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// 将文件分割成m个
// 一共有 n 个reduce
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := New()
	// Your code here.

	c.server()
	return c
}
func New() *Coordinator {
	return &Coordinator{
		workers: map[WorkerID]WorkerType{},
	}

}
