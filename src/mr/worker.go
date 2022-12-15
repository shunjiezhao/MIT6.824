package mr

import (
	"context"
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(log.Llongfile)
	// Your worker implementation here.
	ctx := context.Background()
	// 1. 注册
	workerID := Register()
	go workerID.server(ctx)

work:
	fmt.Println(workerID, "开始获取任务")
	var reply AskResp
	// id 是map 或者 reduce 的第几号任务
	var masterS bool
	for true {
		reply, masterS = Ask(workerID)
		if masterS == false { // master 结束工作 任务完成
			log.Printf("worker: %v receive master down\n", workerID)
			return
		}

		if reply.Type.CanWork() { // 后备任务
			break //可以工作了
		}
		time.Sleep(1 * time.Second)
	}
	// 有工作
	log.Printf("worker:%v start %v:%v work \n", workerID, reply.Type.String(), reply.Id)
	// 检查我的工作内容是什么
	// 读取输入文件

	if reply.Type == MapW {
		worker := newMapWorker(&reply)
		worker.work(workerID, mapf, reply, ctx)
	} else if reply.Type == ReduceW {
		worker := newReduceWorker(&reply)
		worker.work(workerID, reducef, reply, ctx)
	}
	// 继续获取任务
	goto work
}

func Commit(id WorkerID, gid GroupID, Type WorkerType, files []string) error {
	args := CommitReq{
		Id:    id,
		GID:   gid,
		Type:  Type,
		Files: files,
	}
	reply := CommitResp{}
	log.Println("Commit")
	ok := call("Coordinator.Commit", &args, &reply)
	log.Println("Commit 完成")
	if ok {
		log.Printf("%v-worker: %v commit success\n", Type.String(), id)
	} else {
		log.Printf("%v-worker: %v commit failed\n", Type.String(), id)
		return fmt.Errorf("commit failed")
	}
	return nil
}

// 返回工人编号
func Register() WorkerID {
	args := RegReq{}
	reply := RegResp{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		log.Printf("client receive: %v\n", reply)
	} else {
		log.Printf("call failed!\n")
	}
	return reply.Id
}
func Ask(workerID WorkerID) (reply AskResp, workStatus bool) {
	args := AskReq{workerID}

	workStatus = call("Coordinator.Ask", &args, &reply)
	if workStatus {
		log.Printf("client receive: %v\n", reply)
	} else {
		log.Printf("call failed!\n")
	}
	return reply, workStatus
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
func callWorker(id WorkerID, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	log.Printf("\n发送文件数量为%v\n", len(args.(*ReduceRevReq).Files))

	sockname := workerSock(id)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Fatalln(err)
	return false
}
