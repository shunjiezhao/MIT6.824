package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MapWorker struct {
	tmpFiles  []string //中间的文件名
	mCnt      int      // map 的数量
	rCnt      int      //reduce 的数量
	inPutFile string
	gID       GroupID
}

func newMapWorker(resp *AskResp) *MapWorker {
	return &MapWorker{
		tmpFiles:  []string{},
		mCnt:      resp.MapCnt,
		rCnt:      resp.ReduceCnt,
		inPutFile: resp.InputFileName,
		gID:       resp.Id,
	}
}
func (m *MapWorker) work(id WorkerID, mapf func(string, string) []KeyValue, reply AskResp) {
	go m.server()
	if reply.InputFileName == "" {
		panic("没有得到输入文件名")
	}
	log.Printf("%v工人得到输入文件: %v", reply.Type.String(), reply.InputFileName)
	file, err := os.Open(reply.InputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.InputFileName)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.InputFileName)
	}
	intermediate := mapf(reply.InputFileName, string(content))
	// 根据产生的中间键将其分成 r 个文件
	// 1, 创建文件 记录临时文件名
	rCnt := reply.ReduceCnt
	files, fileName := make([]*os.File, rCnt), make([]string, rCnt)
	for i := 0; i < rCnt; i++ {
		fileName[i] = fmt.Sprintf("%v-%v", reply.TmpfileName, i)
		log.Printf("%v-worker-%v: create tmp file: %v", reply.Type.String(), reply.Id, fileName[i])
		f, err := os.Create(fileName[i])
		defer f.Close()
		if os.IsExist(err) {
			f.Close()
			f, _ = os.Open(fileName[i])
			defer f.Close()
		}
		if err != nil {
			panic(fmt.Sprintf("create tmp file failed: %v\n", err))
		}
		f.Seek(0, 0) // 冲头开始
		files[i] = f
		defer f.Close()
	}

	for i := 0; i < len(intermediate); i++ {
		idx := ihash(intermediate[i].Key) % reply.ReduceCnt // hash % R
		files[idx].WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, intermediate[i].Value))
	}
	for i := 0; i < rCnt; i++ {
		files[i].Close()
	}
	// 都ok
	// 发送成功
	assert(m.gID == reply.Id, "map的组内id不一致，检查是否赋值")
	Commit(id, m.gID, MapW, fileName)
}

// reduce 读取文件
func (m *MapWorker) reciveFile() {
}
func (m *MapWorker) server() {
	return
	log.Printf("map工人%v： 正在监听🚀", m.gID)
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := mapWorkerSock(m.gID)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
