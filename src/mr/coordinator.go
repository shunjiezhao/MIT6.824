package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	workerID WorkerID // 分配给每个工人
	ticker   time.Duration

	mapW    completeStatus
	reduceW completeStatus

	wMap       sync.RWMutex
	workers    map[WorkerID]workerStatus // 维护状态 然后需要 记录活跃的工人
	tmpFiles   [][]string
	tmpPath    string
	inputPath  string
	outputPath string
	done       bool
	cond       *sync.Cond
}

func (c *Coordinator) getMapCount() int {
	return c.mapW.sum
}
func (c *Coordinator) getReduceCount() int {
	return c.reduceW.sum
}

type completeStatus struct {
	now      int    // 当前有多少个人占了坑位
	complete int    // 完成的总数
	sum      int    // 总共的总数
	bitMap   []bool // 一共有多少个坑位，有哪些坑位已经被占住了
}
type workerStatus struct {
	status WorkerStatus
	Type   WorkerType
	gID    GroupID // 组内id
}

// 根据 传入的组内 id 得到其 输入文件名
func (c *Coordinator) getMapInputFName(id GroupID) string {
	if int(id) > c.getMapCount() {
		log.Println("id in group is bigger than amount of the map workers ")
		return ""
	}
	os.MkdirAll(c.inputPath, 0777)
	return fmt.Sprintf(path.Join(c.inputPath, fmt.Sprintf("mr-in-%v", id)))
}
func (c *Coordinator) getMapTmpFName(id GroupID) string {
	os.MkdirAll(c.tmpPath, 0777)
	return fmt.Sprintf(path.Join(c.tmpPath, fmt.Sprintf("mr-tmp-%v", id)))
}
func (c *Coordinator) getReOutPutFName(id GroupID) string {
	os.MkdirAll(c.outputPath, 0777)
	return fmt.Sprintf(path.Join(c.outputPath, fmt.Sprintf("mr-out-%v", id)))
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
func (c *Coordinator) Commit(args *CommitReq, reply *CommitResp) error {
	//TODO:通知 reduce 任务
	log.Printf("ID: %v %v-worker: %v  commit\n", args.Id, args.Type.String(), args.GID)
	c.wMap.Lock()
	defer c.wMap.Unlock()
	if args.Type == MapW {
		c.mapW.complete++
		if len(args.Files) != c.reduceW.sum {
			log.Println("want: %v ; but: %v\n", c.reduceW.sum, len(args.Files))
			panic("map 任务生成的临时文件数量和 reduce 数量不一致")
		}
		for i := 0; i < c.reduceW.sum; i++ {
			c.tmpFiles[i] = append(c.tmpFiles[i], args.Files[i]) // 对于每一个 reduce 添加
		}
		if c.mapW.complete == c.mapW.sum {
			log.Println("map已经做完")
			//TODO:通知所有的reduce
			go func() {
				var resp Empty
				var cnt int
				for id, status := range c.workers {
					if status.Type == ReduceW {
						assert(int(status.gID) < c.getReduceCount(), "组内id大于reduce工人的数量")
						req := ReduceRevReq{Files: c.tmpFiles[status.gID]}
						fmt.Println("GID:", status.gID)
						assert(c.getMapCount() == len(c.tmpFiles[status.gID]),
							fmt.Sprintf("向 reduce发送 %v 个worker 传递信号的; 但是发送了 %v", c.getMapCount(),
								len(c.tmpFiles[status.gID])))
						callWorker(id, reduceWKReceive(status.gID), &req, &resp)
						cnt++
					}
				}
				assert(cnt == c.getReduceCount(), fmt.Sprintf("应该向 %v 个worker 传递信号的 但是向 %v 个传递信号", c.getReduceCount(),
					cnt))
			}()
		}
	} else if args.Type == ReduceW {
		c.reduceW.complete++
		if c.reduceW.complete == c.getReduceCount() {
			log.Println("reduce已经做完")
			//工作已经全部做完，唤醒主线程
			c.cond.Signal()
		}
	}
	return nil
}

func assert(check bool, content string) {
	if !check {
		panic(content)
	}

}
func (c *Coordinator) Ask(args *AskReq, reply *AskResp) error {
	log.Printf("worker:%v 请求任务\n", args.Id)
	// 检查是否有任务可以分配
	status := c.CheckCanAlloc(args.Id)
	if status.Type == WorkTNODefine {
		log.Println("all tasks are done by workers")
		return nil
	}
	log.Printf("分配%v-%v任务给工人\n", status.Type.String(), status.gID)
	//TODO：将分配的任务注册并分配 各个任务（map or reduce）的组内id
	// Map 还需返回输入文件名
	reply.Type = status.Type
	reply.Id = status.gID
	reply.ReduceCnt = c.getReduceCount()
	reply.MapCnt = c.getMapCount()
	if status.Type == MapW {
		reply.InputFileName = c.getMapInputFName(status.gID)
		reply.TmpfileName = c.getMapTmpFName(status.gID)
	} else if status.Type == ReduceW {
		reply.OutPutFileName = c.getReOutPutFName(status.gID)
	}
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
	c.workers[reply.Id] = workerStatus{status: WorkerFree, Type: 0, gID: 0}
	return nil
}
func (c *Coordinator) Pong() error {
	ticker := time.NewTicker(c.ticker)
	for {
		select {
		case <-ticker.C:
			c.wMap.Lock()
			var ping = PingReq{Content: "Ping"}
			for id, status := range c.workers {
				var pong PingResp
				call("WorkerID.Ping", &ping, &pong)
				if pong.Content != "Pong" {
					if status.Type == MapW {
						c.mapW.now--
					} else if status.Type == ReduceW {
						c.reduceW.now--
					}
					c.workers[id] = workerStatus{status: Dead}
					log.Println("工人:%d ping 不通\n", id)

				}
			}
			c.wMap.Unlock()
		}
	}
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
	c.cond.L.Lock()
	for c.reduceW.complete != c.getReduceCount() {
		c.cond.Wait()
	}
	// Your code here.
	log.Println("done")
	ret = true
	return ret
}

// 分配任务
func (c *Coordinator) CheckCanAlloc(wid WorkerID) workerStatus {
	c.wMap.Lock()
	defer c.wMap.Unlock()
	if c.workers[wid].status == Dead {
		log.Println("dead?")
	}
	// 检查是否可以分配 map
	if c.mapW.sum > c.mapW.now {
		// 说明map 可以分配
		c.mapW.now++
		id := c.chooseMinOne(c.mapW.bitMap)
		status := workerStatus{status: Working, Type: MapW, gID: GroupID(id)}
		c.modidyWorkerStatus(wid, status)
		return status
	}
	if c.reduceW.sum > c.reduceW.now {
		// 说明map 可以分配
		c.reduceW.now++
		id := c.chooseMinOne(c.reduceW.bitMap) // 当前组内可分配的最小id
		status := workerStatus{status: Working, Type: ReduceW, gID: id}
		c.modidyWorkerStatus(wid, status)
		return status
	}

	status := workerStatus{WorkerFree, WorkTNODefine, 0}
	c.modidyWorkerStatus(wid, status)
	return status
}

// 起始分配为0

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// 将文件分割成m个
// 一共有 n 个reduce
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := New(nReduce)
	// Your code here.

	// 将数据切成n片并将其写入文件 文件名通过 ask 发送给 map worker 或者 reduce worker
	// 命名格式为 mr-in-%v
	// map 工人为 10个?
	// 删除可能存在的文件
	c.removeExistInputFile()
	c.writeInputFile(files)

	c.server()
	return c
}
func (c *Coordinator) writeInputFile(files []string) {
	// 读出所有的文件然后 切割成等量的大小
	var b []byte
	for _, file := range files {
		bytes, err := ioutil.ReadFile(file)
		if err != nil {
			panic(err)
		}
		b = append(b, bytes[:]...)
	}

	cnt := len(b) / c.getMapCount()
	log.Printf("count : %v", cnt)
	for i, idx := 0, 0; i < c.getMapCount(); i++ {
		fileName := c.getMapInputFName(GroupID(i))
		end := idx + cnt
		if idx+cnt > len(b) {
			end = len(b)
			if i != c.getMapCount()-1 {
				panic("should be the lastest")
			}
		}
		if err := ioutil.WriteFile(fileName, b[idx:end], 0777); err != nil {
			log.Printf("can not create file: %v", err)
			panic("")
		}
		log.Printf("create %v successful", fileName)
		idx += cnt
	}
}

func (c *Coordinator) removeExistInputFile() {
	for i := 0; i < c.mapW.sum; i++ {
		fileName := c.getMapInputFName(GroupID(i))
		os.Remove(fileName)
	}
}

// 必须持有 map 这个锁才能修改
func (c *Coordinator) modidyWorkerStatus(wid WorkerID, status workerStatus) {
	c.workers[wid] = status
}

func (c *Coordinator) chooseMinOne(ids []bool) GroupID {
	for i := 0; i < len(ids); i++ {
		if ids[i] == false {
			ids[i] = true
			return GroupID(i)
		}
	}
	return -1
}

func New(nReduce int) *Coordinator {
	nReduce = 3
	mapSum := 1
	return &Coordinator{
		ticker:     time.Second,
		tmpPath:    "/home/zsj/mit/6.824/src/main/mr-tmp",
		inputPath:  "/home/zsj/mit/6.824/src/main/mr-input",
		outputPath: "/home/zsj/mit/6.824/src/main/mr-output",
		mapW: completeStatus{
			sum:    mapSum,
			bitMap: make([]bool, mapSum),
		},
		reduceW: completeStatus{
			sum:    nReduce,
			bitMap: make([]bool, nReduce),
		},
		tmpFiles: make([][]string, nReduce),
		workers:  map[WorkerID]workerStatus{},
		cond:     &sync.Cond{L: &sync.Mutex{}},
	}
}
