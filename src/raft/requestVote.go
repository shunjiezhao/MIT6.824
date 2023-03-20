package raft

import (
	"sync"
	"time"
)

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must index0 with capital letters!
type RequestVoteArgs struct {
	// Your data here (1A, 2B).
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§4.4)
	LastLogTerm  int //term of candidate’s last log entry (§4.4)

}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must index0 with capital letters!
type RequestVoteReply struct {
	// Your data here (1A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as rece:iver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curTerm := rf.currentTerm
	reply.Term = curTerm
	//如果term < currentTerm返回 false
	if curTerm > args.Term { // 过时
		return
	}
	// 所有的服务器都 适用
	if rf.curTermLowL(args.Term) {
		rf.votedFor = -1
		Debug(rf, dError, "%s 过期", rf.Name())
	}

	//这轮 已经投过别人了
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	if rf.compareLogL(args) == false {
		return
	}

	Debug(rf, dVote, "%s[%s,%d] 投票给  %s[%d]", rf.Name(), rf.State(), rf.currentTerm, getServerName(args.CandidateId),
		args.Term)

	rf.votedFor = args.CandidateId
	rf.state = Candidate
	reply.VoteGranted = true
	rf.refreshElectionTime()
	return
}

func (rf *Raft) SendVoteRequestL() {
	var count = 1
	wg := &sync.WaitGroup{}
	// 通知
	for other, _ := range rf.peers {
		if other != rf.me {
			wg.Add(1)
			nIdx := rf.log.lastLogIndex() // last log index
			req := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: nIdx,
				LastLogTerm:  rf.log.entryAt(nIdx).Term,
			}
			go rf.sendVoteRequest(wg, &count, other, &req)
		}
	}
	go func() {
		wg.Wait()
		rf.mu.Lock()
		if rf.state == Candidate {
			rf.electionTime = time.Now() // 再次选举
			Debug(rf, dWarn, "%s 选举失败 retry selection", rf.Name())
		}
		rf.mu.Unlock()
	}()
}
func (rf *Raft) sendVoteRequest(wg *sync.WaitGroup, count *int, other int, args *RequestVoteArgs) {
	defer wg.Done()
	var reply RequestVoteReply

	call := rf.sendRequestVote(other, args, &reply)
	Debug(rf, dVote, "%s -> % s[call:%t] req: %+ ans: %+v", rf.Name(), getServerName(other), call, args, reply)

	assert(!call && reply.VoteGranted == true)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curTermLowL(reply.Term) {
		return // 过时立即转变 + 返回
	} else {
		if reply.VoteGranted == false {
			Debug(rf, dError, "%s is 没有获取到 %s 票", rf.Name(), getServerName(other))
		} else {
			Debug(rf, dVote, "%s is 获取到 %s 票 count:%v", rf.Name(), getServerName(other), *count)
			*count++
			if *count > len(rf.peers)/2 && rf.state == Candidate {
				Debug(rf, dVote, "%s now is leader", rf.Name())
				rf.state = Leader
				rf.AppendMsgL(true) // 发送心跳
				rf.freshNextSliceL()
			}
		}
	}
}

// 比较 rpc 请求的日条目是否比自己的新，或者一样新
// 返回true 代表可以给他投票
func (rf *Raft) compareLogL(args *RequestVoteArgs) bool {
	// 日志条目的比较

	lastIndex := rf.log.lastLogIndex()
	if args.LastLogTerm > rf.log.entryAt(lastIndex).Term {
		return true
	}
	if args.LastLogTerm < rf.log.entryAt(lastIndex).Term {
		return false
	}

	// term 相等比较
	return args.LastLogIndex >= lastIndex
}
func max(a, b int) int {
	return -min(-a, -b)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
