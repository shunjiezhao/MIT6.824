package mr

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
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
	isRecieve  uint32
	lock       sync.RWMutex
}

func reduceWKReceive(gID GroupID) string {
	return workerRpcName(ReduceW, gID) + ".Receive"
}

func (r *ReduceWorker) server(ctx context.Context) {
	server(ctx, r.gID, ReduceW, r)
}

func newReduceWorker(resp *AskResp) *ReduceWorker {
	return &ReduceWorker{
		cond:       &sync.Cond{L: &sync.Mutex{}},
		mCnt:       resp.MapCnt,
		outPutFile: resp.OutPutFileName,
		gID:        resp.Id,
	}
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (r *ReduceWorker) work(id WorkerID, reducef func(string, []string) string, reply AskResp, ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	go r.server(ctx)
	defer cancel()
	//当出来时就是已经将所有的文件接受
	r.receiveTmpFile()
	// 开始做事情
	//r.lock.Lock()
	var b []byte
	r.lock.RLock()
	for i := 0; i < r.mCnt; i++ {
		//fmt.Printf("worker-%v:读取文件%v\n", r.gID, r.files[i])
		f, err := os.Open(r.files[i])
		if err != nil {
			panic("打开中间文件失败")
		}
		all, err := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			panic(fmt.Sprintf("reduce工人接受到的文件名无法打开: ", err.Error()))
		}
		b = append(b, all[:]...)
	}
	r.lock.RUnlock()

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
	log.Printf("reduce: %v 工作结束", r.gID)
}
func (r *ReduceWorker) Receive(args *ReduceRevReq, resp *Empty) error {
	log.Printf("调用了 receive")
	r.lock.Lock()
	defer r.lock.Unlock()
	if len(r.files) == r.mCnt {
		r.cond.Signal()
		log.Printf("已经收取够文件了，不用再传\n")
		return nil
	}
	if len(args.Files) == 0 {
		return fmt.Errorf("文件名为空")
	}
	r.files = make([]string, len(args.Files))
	// make a copy of buf_Seq in an entirely separate slice
	copy(r.files, args.Files)
	log.Println("收到长度为: ", len(r.files))
	if len(r.files) != r.mCnt {
		log.Fatalf("reduce接受的文件数量不够; want:%v; but:%v", r.mCnt, len(r.files))
	}
	r.cond.Signal()
	return nil
}
func (r *ReduceWorker) receiveTmpFile() {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	for r.lock.RLock(); len(r.files) != r.mCnt; r.lock.RLock() {
		r.lock.RUnlock()
		log.Printf("wait 进度：%v/%v", len(r.files), r.mCnt)
		r.cond.Wait()
	}
	r.lock.RUnlock()
	log.Printf("signal")
}
