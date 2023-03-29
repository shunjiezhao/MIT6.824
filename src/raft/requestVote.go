package raft

import (
	"sync"
)

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must Start with capital letters!
type RequestVoteArgs struct {
	// Your data here (1A, 2B).
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int //index of candidate’s last Log entry (§4.4)
	LastLogTerm  int //term of candidate’s last Log entry (§4.4)

}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must Start with capital letters!
type RequestVoteReply struct {
	// Your data here (1A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
// 1. Reply false if term < CurrentTerm (§5.1)
// 2. If VotedFor is null or candidateId, and candidate’s Log is at
// least as up-to-date as rece:iver’s Log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 所有的服务器都 适用
	if rf.curTermLowL(args.Term) {
		Debug(rf, dError, "过期")
	}
	curTerm := rf.CurrentTerm

	reply.Term = rf.CurrentTerm
	//如果term < currentTerm返回 false
	if curTerm > args.Term { // 过时
		return
	}

	//这轮 已经投过别人了
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		return
	}

	Debug(rf, dVote, "Compare Last[%+v] %s Last[term: %d, index: %d] ans: %v", rf.Log.lastLog(), getServerName(args.CandidateId),
		args.LastLogTerm, args.LastLogIndex, rf.compareLogL(args))

	if rf.compareLogL(args) == false {
		return
	}

	Debug(rf, dVote, "[%s,%d] 投票给  %s[%d]", rf.State(), rf.CurrentTerm, getServerName(args.CandidateId),
		args.Term)

	reply.VoteGranted = true
	rf.refreshElectionTime()
	//TODO: need to  persist
	rf.VotedFor = args.CandidateId
	rf.state = Candidate
	rf.persist()
	return
}

func (rf *Raft) SendVoteRequestL() {
	var count = 1
	wg := &sync.WaitGroup{}
	// 通知
	for other, _ := range rf.peers {
		if other != rf.me {
			wg.Add(1)
			nIdx := rf.Log.lastLogIndex() // last Log index
			req := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: nIdx,
				LastLogTerm:  rf.Log.entryAt(nIdx).Term,
			}
			go rf.sendVoteRequest(wg, &count, other, &req)
		}
	}
	go func() {
		wg.Wait()
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			rf.retryElectionRefresh() // 再次选举
			Debug(rf, dWarn, "选举失败 retry selection")
		}
	}()
}
func (rf *Raft) sendVoteRequest(wg *sync.WaitGroup, count *int, other int, args *RequestVoteArgs) {
	defer wg.Done()
	var reply RequestVoteReply

	call := rf.sendRequestVote(other, args, &reply)
	Debug(rf, dVote, "-> %v [call:%v] req: %+v ans: %+v", getServerName(other), call, args, reply)

	assert(!call && reply.VoteGranted == true)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curTermLowL(reply.Term) {
		return // 过时立即转变 + 返回
	} else {
		if args.Term != rf.CurrentTerm {
			return
		}

		if reply.VoteGranted == false {
			Debug(rf, dError, "[%d] is 没有获取到 %s[%d] 票", args.Term, getServerName(other), reply.Term)
		} else {
			*count++
			Debug(rf, dVote, "[%d:%s] is 获取到 %s[%d:] 票 count:%v", args.Term, rf.state, getServerName(other), reply.Term, *count)
			if *count > len(rf.peers)/2 && rf.state == Candidate {
				rf.becomeLeaderThenDoL()
			}
		}
	}
}
func (rf *Raft) becomeLeaderThenDoL() {
	Debug(rf, dVote, " now is leader")
	rf.state = Leader
	//TODO: need to  persist
	rf.persist()
	rf.freshNextSliceL()
	rf.AppendMsgL(true) // 发送心跳
}

// 比较 rpc 请求的日条目是否比自己的新，或者一样新
// 返回true 代表可以给他投票
func (rf *Raft) compareLogL(args *RequestVoteArgs) bool {
	// 日志条目的比较
	lastIndex := rf.Log.lastLogIndex()
	lastTerm := rf.Log.entryAt(lastIndex).Term
	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}

	// term 相等比较
	return args.LastLogIndex >= lastIndex
}

func (rf *Raft) updCmtIdxALastAppliedL(index int) {
	prevA, prevC := rf.lastApplied, rf.CommitIndex
	rf.CommitIndex = index
	rf.lastApplied = min(rf.lastApplied, rf.CommitIndex)
	Debug(rf, dInfo, "updCmtIdxALastAppliedL: lastApplied: %d -> %d lastCommit: %d -> %d", prevA, rf.lastApplied, prevC, rf.CommitIndex)
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
