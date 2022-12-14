package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
)

type ReduceWorker struct {
	cond       *sync.Cond // 用来 唤醒 reduce 任务的
	files      []string
	mCnt       int // map 的数量
	outPutFile string
	gID        GroupID
}

func reduceWKReceive(id GroupID) string {
	return fmt.Sprintf("ReduceWorker-%v.Receive", id)
}

func (r *ReduceWorker) server() {
	log.Printf("reduce工人：%v 正在监听🚀", r.gID)
	name := fmt.Sprintf("ReduceWorker-%v", r.gID)
	rpc.RegisterName(name, r)
	server := rpc.NewServer()
	server.HandleHTTP("/"+name, "/"+name+"/debug/rpc")
	sockname := reduceWorkerSock(r.gID)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func newReduceWorker(resp *AskResp) *ReduceWorker {
	return &ReduceWorker{
		cond:       &sync.Cond{L: &sync.Mutex{}},
		files:      []string{},
		mCnt:       resp.MapCnt,
		outPutFile: resp.OutPutFileName,
		gID:        resp.Id,
	}
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (r *ReduceWorker) work(id WorkerID, reducef func(string, []string) string, reply AskResp) {
	go r.server()

	//TODO:询问是否完成工作
	r.receiveTmpFile()
	// 开始做事情
	var b []byte
	for i := 0; i < r.mCnt; i++ {
		fmt.Printf("worker-%v:读取文件%v\n", r.gID, r.files[i])
		all, err := ioutil.ReadFile(r.files[i])
		if err != nil {
			panic(fmt.Sprintf("reduce工人接受到的文件名无法打开: ", err.Error()))
		}
		b = append(b, all[:]...)
	}
	lines := strings.Split(string(b), "\n")
	fmt.Println("lines:", len(lines[0]))
	fmt.Println("lines: ", len(strings.Split(lines[0], " ")))
	var intermediate []KeyValue
	for i := 0; i < len(lines); i++ {
		word := strings.Split(lines[i], " ")

		if len(word) == 0 {
			fmt.Println("len(word) = 0, i :", i)
			continue
		} else if len(word) == 1 {
			fmt.Printf("len(word) = 1,word: %v;  i :%v\n", word[0], i)
			continue
		}
		if len(word) != 2 {
			fmt.Printf("%v", word)
			log.Printf("切分后的长度为:%v\n", len(word))
			panic("检查单词格式")
		}
		intermediate = append(intermediate, KeyValue{
			Key:   word[0],
			Value: word[1],
		})
	}
	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(r.outPutFile)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	Commit(id, reply.Id, reply.Type, nil)
	ofile.Close()
}
func (r *ReduceWorker) Receive(args *ReduceRevReq, resp *Empty) error {
	log.Printf("调用了 receive")
	if len(args.Files) == 0 {
		return fmt.Errorf("文件名为空")
	}

	r.files = args.Files
	if len(r.files) != r.mCnt {
		log.Fatalf("reduce接受的文件数量不够; want:%v; but:%v", r.mCnt, len(r.files))
	}
	r.cond.Signal()
	return nil
}
func (r *ReduceWorker) receiveTmpFile() {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	for len(r.files) != r.mCnt {
		r.cond.Wait()
	}
}
