package raft

import (
	"fmt"
)

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("append args: id:%d term:%d  commit: %d prev:[%d:%d] entries: %v ", a.LeaderId, a.Term, a.LeaderCommit, a.PrevLogIndex, a.PrevLogTerm, a.Entries)
}

// example RequestVote RPC reply structure.
// field names must index0 with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) printAppendReplyLog(args *AppendEntriesArgs) {
	var ty logTopic = dLog
	if juegeHeart(args.Entries) {
		ty = DHeart
	}
	Debug(rf, ty, "%s[%s]<- %s [term: %d]: %v ", rf.Name(), rf.State(), getServerName(args.LeaderId), args.Term, args)
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var prev = 0 // debug

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	rf.printAppendReplyLog(args)
	// 所有服务器遵守的规则
	if rf.curTermLowL(args.Term) {
		return
	}

	if rf.currentTerm > args.Term { // rule 1
		Debug(rf, dWarn, "%s found leader %s 过期", rf.Name(), getServerName(args.LeaderId))
		reply.Success = false
		//如果一个节点接收了一个带着过期的任期号的请求，那么它会拒绝这次请求。
		// 如果这个 leader 的任期号小于这个 candidate 的当前任期号，那么这个 candidate 就会拒绝这次 RPC，然后继续保持 candidate 状态。
		return
	}

	//返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	//（译者注：在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）
	if rf.log.lastLogIndex() < args.PrevLogIndex || rf.log.entryAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		Debug(rf, dWarn, "%s <- %s 传递的日志不连续", rf.name, getServerName(args.LeaderId))
		return
	}
	//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1 //next
		if rf.log.lastLogIndex() < index {
			Debug(rf, dLog, "%s append log entry", rf.name)
			rf.AppendLogL(entry) // 追加日志中尚未存在的任何新条目
			continue
		}

		// index < last index 所以冲突
		if rf.log.entryAt(index).Term != entry.Term {
			Debug(rf, dWarn, "%s cut log entry", rf.name)
			prevL := rf.log.lastLogIndex()
			rf.log.cut2end(index - 1)
			panicIf(prevL == rf.log.lastLogIndex(), "")
			rf.AppendLogL(entry)
		} else {
			continue // term 相等 并且 index 相等
		}

	}

	prev = rf.commitIndex
	// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），
	if args.LeaderCommit > rf.commitIndex {
		// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLogIndex())
	}

	Debug(rf, dCommit, "%s <- [%s] update commit Index: %d -> %d", rf.name, getServerName(args.LeaderId), prev, rf.commitIndex)

	//如果这个 leader 的任期号（这个任期号会在这次 RPC 中携带着）不小于这个 candidate 的当前任期号，那么这个 candidate 就会觉得这个 leader 是合法的，然后将自己转变为 follower 状态。
	// 当前任期 curTerm = args.Term
	rf.state = Follower
	rf.refreshElectionTime() // 更新心跳时间
	reply.Success = true
	if rf.shouldApplyL() {
		rf.signLogEnter()
	}
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
		if heart == false {
			Debug(rf, dLog2, "%s send log to %s [%d: %d]", rf.name, getServerName(i), rf.nextIndex[i], rf.log.lastLogIndex())
			if rf.nextIndex[i] <= rf.log.lastLogIndex() {
				log = rf.log.cloneRange(rf.nextIndex[i], rf.log.lastLogIndex())
			} else {
				continue // 没有日志可以发送
			}
		}

		panicIf(juegeHeart(log) != heart, "")
		rf.SendLogLG(i, log)
	}
}

func (rf *Raft) SendLogLG(idx int, entries []LogEntry) {
	nIndex := rf.nextIndex[idx] - 1
	if nIndex > rf.log.lastLogIndex() {
		nIndex = rf.log.lastLogIndex()
	}
	var args = &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nIndex,
		PrevLogTerm:  rf.log.entryAt(nIndex).Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}

	go rf.appendMsg(idx, args) // 发送 rpc
}

func (rf *Raft) procAppendReplyL(idx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	panicIf(rf.state != Leader, "")

	if rf.curTermLowL(reply.Term) {
		// 如果一个 candidate 或者 leader 发现自己的任期号过期了，它就会立刻回到 follower 状态。
		return
	}

	if args.Term != rf.currentTerm {
		return
	}
	if juegeHeart(args.Entries) {
		return
	}

	if reply.Success {
		panicIf(rf.state != Leader, "")
		// 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
		rf.nextIndex[idx] = max(args.PrevLogIndex+len(args.Entries), rf.nextIndex[idx])
		rf.matchIndex[idx] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[idx])
		Debug(rf, dLeader, "%s update %s next: %d match: %d", rf.Name(), getServerName(idx), rf.nextIndex[idx], rf.matchIndex[idx])
	} else {
		Debug(rf, dLog, "%s retry to send log to %s", rf.name, getServerName(idx))
		rf.nextIndex[idx] = max(1, rf.nextIndex[idx]-1)                                // 如果因为日志不一致而失败，则 nextIndex 递减并重试
		rf.SendLogLG(idx, rf.log.cloneRange(rf.nextIndex[idx], rf.log.lastLogIndex())) //
	}

	rf.updateCommitIndexL()
}
func (rf *Raft) updateCommitIndexL() {
	panicIf(rf.state != Leader, "")
	Debug(rf, dInfo, "%s update commit index; before: %d", rf.Name(), rf.commitIndex)
	start := max(rf.commitIndex+1, rf.log.start())

	for index := start; index <= rf.log.lastLogIndex(); index++ {
		var count = 1
		for perr, _ := range rf.peers {
			if perr != rf.me && rf.matchIndex[perr] >= index {
				count++
			}
		}
		// 假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，则令 commitIndex = N（5.3 和 5.4 节）
		if count > len(rf.peers)/2 && rf.log.entryAt(index).Term == rf.currentTerm {
			rf.commitIndex = index
		}
	}
	Debug(rf, dCommit, "%s update commit index; after: %d", rf.Name(), rf.commitIndex)
	rf.signLogEnter()
}
