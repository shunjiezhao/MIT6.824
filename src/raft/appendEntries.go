package raft

import (
	"fmt"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of Log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("append args: id:%d term:%d  commit: %d prev:[%d:%d] entries: %v ", a.LeaderId, a.Term, a.LeaderCommit, a.PrevLogIndex, a.PrevLogTerm, a.Entries)
}

// example RequestVote RPC reply structure.
// field names must Start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// optimize log copy when log don't match
	XTerm  int // conflict term
	XIndex int // conflict index
	XLen   int // conflict len
}

func (rf *Raft) printAppendReplyLog(args *AppendEntriesArgs) {
	var ty logTopic = dLog
	if juegeHeart(args.Entries) {
		ty = DHeart
	}
	Debug(rf, ty, "%s[%s:%d]<- %s [term: %d]: %v ", rf.Name(), rf.State(), rf.CurrentTerm, getServerName(args.LeaderId), args.Term, args)
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var prev = 0 // debug

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.printAppendReplyLog(args)

	if rf.CurrentTerm > args.Term { // rule 1
		reply.Term = rf.CurrentTerm
		Debug(rf, dWarn, "%s found leader %s 过期", rf.Name(), getServerName(args.LeaderId))
		reply.Success = false
		//如果一个节点接收了一个带着过期的任期号的请求，那么它会拒绝这次请求。
		// 如果这个 leader 的任期号小于这个 candidate 的当前任期号，那么这个 candidate 就会拒绝这次 RPC，然后继续保持 candidate 状态。
		return
	}

	// 所有服务器遵守的规则
	if args.Term > rf.CurrentTerm {
		rf.termLowL(args.Term)

	}

	reply.Term = rf.CurrentTerm
	rf.refreshElectionTime() // 当前term的合法leader

	//返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	//（译者注：在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）
	if rf.Log.lastLogIndex() < args.PrevLogIndex {
		// 说明 太短
		reply.Success = false
		reply.XLen = len(rf.Log.Logs)
		Debug(rf, dWarn, "%s <- %s 传递的日志不连续 设置 Xlen: %d", rf.name, getServerName(args.LeaderId), reply.XLen)
		return
	}

	prevLogTerm := rf.Log.entryAt(args.PrevLogIndex).Term
	if prevLogTerm != args.PrevLogTerm {
		reply.XTerm = prevLogTerm
		reply.XIndex = rf.Log.search(prevLogTerm)
		reply.Success = false
		Debug(rf, dWarn, "%s <- %s 传递的日志不连续 Xindex: %d Xterm: %d", rf.name, getServerName(args.LeaderId), reply.XIndex, reply.XTerm)
		return
	}

	prev = rf.commitIndex
	//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1 //next
		if rf.Log.lastLogIndex() < index {
			panicIf(rf.Log.lastLogIndex()+1 != index, "不连续")
			Debug(rf, dLog, "%s append %d Log entry %+v", rf.name, index, entry)
			rf.AppendLogL(entry) // 追加日志中尚未存在的任何新条目

			continue
		}

		// index < last index 所以冲突
		if rf.Log.entryAt(index).Term != entry.Term {
			Debug(rf, dWarn, "%s cut Log entry [0, %d]", rf.name, index-1)
			prevL := rf.Log.lastLogIndex()
			rf.Log.cut2end(index - 1) // 可能被覆盖
			panicIf(rf.Log.lastLogIndex() != index-1, "cut error")
			if rf.commitIndex > rf.Log.lastLogIndex() { // fix < with >
				rf.updCmtIdxALastAppliedL(rf.Log.lastLogIndex())
			}
			panicIf(prevL == rf.Log.lastLogIndex(), "")
			panicIf(rf.Log.lastLogIndex() == index, "rf Log not cut")
			rf.AppendLogL(entry)
			Debug(rf, dLog, "%s append %d Log entry %+v", rf.name, index, entry)
		} else {
			continue // term 相等 并且 index 相等
		}

	}

	if args.LeaderCommit > rf.commitIndex {
		// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），
		rf.updCmtIdxALastAppliedL(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	}
	Debug(rf, dCommit, "%s <- [%s] update commit Index: %d -> %d", rf.name, getServerName(args.LeaderId), prev, rf.commitIndex)
	//如果这个 leader 的任期号（这个任期号会在这次 RPC 中携带着）不小于这个 candidate 的当前任期号，那么这个 candidate 就会觉得这个 leader 是合法的，然后将自己转变为 follower 状态。
	// 当前任期 curTerm = args.Term

	rf.state = Follower
	rf.persist()
	reply.Success = true
	rf.signLogEnter()
	return
}

func juegeHeart(log []LogEntry) bool {
	return log == nil || len(log) == 0
}

func (rf *Raft) printCallLog(idx int, args *AppendEntriesArgs, call bool, reply *AppendEntriesReply) {
	if juegeHeart(args.Entries) {
		Debug(rf, DHeart, "%s -> %s call:%v req: %+v ans %+v", rf.Name(), getServerName(idx), call, args, reply)
	} else {
		Debug(rf, DHeart, "%s -> %s call:%v req: %+v ans %+v", rf.Name(), getServerName(idx), call, args, reply)
	}
}

// rpc 调用，返回reply.Success 是否成功
func (rf *Raft) appendMsg(idx int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	call := rf.appendEntries(idx, args, &reply)
	rf.printCallLog(idx, args, call, &reply)

	if !call {
		panicIf(reply.Success == true || reply.Term != 0, "Leader: rpc call failed but get ans")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curTermLowL(reply.Term) {
		// 如果一个 candidate 或者 leader 发现自己的任期号过期了，它就会立刻回到 follower 状态。
		return
	}

	if args.Term != rf.CurrentTerm {
		return
	}

	if rf.state == Leader {
		rf.procAppendReplyL(idx, args, &reply)
	}
}
func (rf *Raft) AppendMsgL(heart bool) {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		var log []LogEntry

		if !heart {
			if rf.nextIndex[i] <= rf.Log.lastLogIndex() {
				Debug(rf, dLog, "%s send Log to %s [%d: %d]", rf.name, getServerName(i), rf.nextIndex[i], rf.Log.lastLogIndex())
				log = rf.Log.cloneRange(rf.nextIndex[i], rf.Log.lastLogIndex())
			}
		}

		rf.SendLogLG(i, log)
	}
}

func (rf *Raft) SendLogLG(idx int, entries []LogEntry) {
	nIndex := rf.nextIndex[idx] - 1
	if nIndex > rf.Log.lastLogIndex() {
		nIndex = rf.Log.lastLogIndex()
	}
	var args = &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nIndex,
		PrevLogTerm:  rf.Log.entryAt(nIndex).Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}

	go rf.appendMsg(idx, args) // 发送 rpc
}

func (rf *Raft) procAppendReplyL(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	panicIf(rf.state != Leader, "")

	if reply.Success {
		// 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
		rf.nextIndex[idx] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[idx])
		rf.matchIndex[idx] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[idx])
		Debug(rf, DIndex, "%s update %s next: %d match: %d", rf.Name(), getServerName(idx), rf.nextIndex[idx], rf.matchIndex[idx])
	} else {
		prev := rf.nextIndex[idx]
		if reply.XLen != 0 {
			rf.nextIndex[idx] = reply.XLen
			Debug(rf, dWarn, "%s 1: retry to send Log to %s( udpate next index %d -> %d", rf.name, getServerName(idx), prev, rf.nextIndex[idx])
		} else {
			if rf.Log.contain(reply.XTerm) {
				rf.nextIndex[idx] = rf.Log.TermLastIndex(reply.XTerm)
				Debug(rf, dWarn, "%s 2: retry to send Log to %s( udpate next index %d -> %d", rf.name, getServerName(idx), prev, rf.nextIndex[idx])
			} else {
				rf.nextIndex[idx] = reply.XIndex
				Debug(rf, dWarn, "%s 3: retry to send Log to %s( udpate next index %d -> %d", rf.name, getServerName(idx), prev, rf.nextIndex[idx])
			}
		}
		rf.nextIndex[idx] = max(rf.nextIndex[idx], 1)

		rf.SendLogLG(idx, rf.Log.cloneRange(rf.nextIndex[idx], rf.Log.lastLogIndex())) //
	}

	rf.updateCommitIndexL()
}

// 更新commitIndex
func (rf *Raft) updateCommitIndexL() {
	panicIf(rf.state != Leader, "")
	prev := rf.commitIndex // for debug
	start := max(rf.commitIndex+1, rf.Log.start())

	for index := start; index <= rf.Log.lastLogIndex(); index++ {
		if rf.Log.entryAt(index).Term != rf.CurrentTerm {
			continue
		}
		var count = 1
		for perr, _ := range rf.peers {
			if perr != rf.me && rf.matchIndex[perr] >= index {
				count++
			}
		}
		// 假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == CurrentTerm 成立，则令 commitIndex = N（5.3 和 5.4 节）
		if count > len(rf.peers)/2 {
			rf.commitIndex = index
		}
	}
	Debug(rf, DIndex, "%s update commit index; %d -> %d", rf.Name(), prev, rf.commitIndex)
	rf.signLogEnter()
}
