package mr

import (
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

	// Your worker implementation here.
	// 1. 注册
	workerID := Register()

	var tp WorkerType
	var id int // id 是map 或者 reduce 的第几号任务
	var masterS bool
	for true {
		tp, id, masterS = Ask(workerID)
		if masterS == false { // master 结束工作 任务完成
			log.Printf("worker: %v receive master down\n", workerID)
			return
		}

		if tp.CanWork() { // 后备任务
			break //可以工作了
		}
		time.Sleep(1 * time.Second)
	}
	// 有工作
	log.Printf("worker:%v start %v:%v work \n", workerID, tp.String(), id)
	// uncomment to send the Example RPC to the coordinator.
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
func Ask(workerID WorkerID) (WorkerType WorkerType, id int, workStatus bool) {
	args := AskReq{workerID}
	reply := AskResp{}

	workStatus = call("Coordinator.Ask", &args, &reply)
	if workStatus {
		log.Printf("client receive: %v\n", reply)
	} else {
		log.Printf("call failed!\n")
	}
	return reply.Type, reply.Id, workStatus
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
