package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of Log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int        // leader’s CommitIndex
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("append args: id:%d term:%d  commit: %d prev:[%d:%d] entries: %v ", a.LeaderId, a.Term, a.LeaderCommit, a.PrevLogIndex, a.PrevLogTerm, a.Entries)
}

// example RequestVote RPC reply structure.
// field names must Start with capital letters!
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// optimize log copy when log don't match
	XTerm  int // conflict term
	XIndex int // conflict index
	XLen   int // conflict len
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var prev = 0 // debug

	// Your code here (2A, 2B).
	local := fmt.Sprintf("append entries 1 %v", time.Now().Unix())
	rf.Lock(local)
	defer rf.Unlock(local)
	Debug(rf, dLog, "[%s:%d]<- %s [term: %d]: %v ", rf.State(), rf.CurrentTerm, getServerName(args.LeaderId), args.Term, args)
	if rf.CurrentTerm > args.Term { // rule 1
		reply.Term = rf.CurrentTerm
		Debug(rf, dWarn, "found leader %s 过期", getServerName(args.LeaderId))
		reply.Success = false
		// 如果一个节点接收了一个带着过期的任期号的请求，那么它会拒绝这次请求。
		// 如果这个 leader 的任期号小于这个 candidate 的当前任期号，那么这个 candidate 就会拒绝这次 RPC，然后继续保持 candidate 状态。
		return
	}

	// 所有服务器遵守的规则
	rf.curTermLowL(args.Term)

	rf.LeaderId = args.LeaderId
	reply.Term = rf.CurrentTerm
	rf.refreshElectionTime() // 当前term的合法leader
	reply.XTerm = -1
	reply.XLen = -1
	reply.XIndex = -1

	//返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	//（译者注：在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）
	if rf.Log.lastLogIndex() < args.PrevLogIndex { // 说明太短
		reply.Success = false
		reply.XLen = rf.Log.lastLogIndex() + 1
		return

	}
	if args.PrevLogIndex < rf.Log.Start { // prevLog 说明之前我们已经进行快照了
		/*
		    虚拟存在的 	1 2 3 4 5
		  	0
		*/
		reply.Success = false
		reply.XIndex = rf.Log.nextLogIndex()
		return
	}
	prevLogTerm := rf.Log.entryAt(args.PrevLogIndex).Term
	if prevLogTerm != args.PrevLogTerm {
		reply.XTerm = prevLogTerm
		reply.XIndex = max(rf.Log.search(prevLogTerm), rf.Log.start())
		reply.Success = false
		return
	}

	prev = rf.CommitIndex
	//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1 //next
		if rf.Log.lastLogIndex() < index {
			Debug(rf, dLog, "append %d Log entry %+v", index, entry)
			rf.AppendLogL(entry) // 追加日志中尚未存在的任何新条目
			continue
		}

		// index < last index 所以冲突
		if rf.Log.entryAt(index).Term != entry.Term {
			Debug(rf, dWarn, "cut Log entry [0, %d]", index-1)
			rf.Log.cut2end(index - 1)                   // 可能被覆盖
			if rf.CommitIndex > rf.Log.lastLogIndex() { // fix < with >
				rf.updCmtIdxALastAppliedL(rf.Log.lastLogIndex())
			}
			rf.AppendLogL(entry)
			Debug(rf, dLog, "append %d Log entry %+v", index, entry)
		} else {
			continue // term 相等 并且 index 相等
		}

	}

	if args.LeaderCommit > rf.CommitIndex {
		// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > CommitIndex），
		rf.updCmtIdxALastAppliedL(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	}
	Debug(rf, dCommit, "<- [%s] update commit Index: %d -> %d lastA: %d", getServerName(args.LeaderId), prev, rf.CommitIndex, rf.lastApplied)
	//如果这个 leader 的任期号（这个任期号会在这次 RPC 中携带着）不小于这个 candidate 的当前任期号，那么这个 candidate 就会觉得这个 leader 是合法的，然后将自己转变为 follower 状态。
	// 当前任期 curTerm = args.Term

	rf.state = Follower
	rf.refreshElectionTime()
	rf.persist()
	reply.Success = true
	rf.signLogEnter()
	return
}

// rpc 调用，返回reply.Success 是否成功
func (rf *Raft) appendMsg(idx int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	call := rf.appendEntries(idx, args, &reply)

	if !call {
		panicIf(reply.Success == true || reply.Term != 0, "Leader: rpc call failed but get ans")
		return
	}

	local := fmt.Sprintf("receive appendMsgL 1 %v", time.Now().Unix())
	rf.Lock(local)
	defer rf.Unlock(local)

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
	rf.signLogEnter()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		var log []LogEntry
		if rf.nextIndex[i] <= rf.Log.lastLogIndex() && // 有东西可以发送
			rf.nextIndex[i] >= rf.Log.start() { // 发送的起点 有 日志
			Debug(rf, dLog, "send Log to %s [%d: %d]", getServerName(i), rf.nextIndex[i], rf.Log.lastLogIndex())
			log = rf.Log.cloneRange(rf.nextIndex[i], rf.Log.lastLogIndex())
			rf.SendLogLG(i, log)
		} else if rf.nextIndex[i] < rf.Log.start() { // 如果说没有日志
			// send snapshot
			Debug(rf, dLog, "send snapshot to %s start: %d", getServerName(i), rf.Log.start())
			rf.sendSnapshotLG(i)
			continue
		} else {
			rf.SendLogLG(i, log)
		}
	}
}

func (rf *Raft) SendLogLG(idx int, entries []LogEntry) {
	nIndex := rf.nextIndex[idx] - 1
	if nIndex < rf.Log.Start {
		nIndex = rf.Log.Start
	}
	if nIndex > rf.Log.lastLogIndex() {
		nIndex = rf.Log.lastLogIndex()
	}
	var args = &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nIndex,
		PrevLogTerm:  rf.Log.entryAt(nIndex).Term,
		LeaderCommit: rf.CommitIndex,
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
		Debug(rf, DIndex, "update %s next: %d match: %d", getServerName(idx), rf.nextIndex[idx], rf.matchIndex[idx])
	} else {

		if reply.XLen != -1 || reply.XTerm != -1 || reply.XIndex != -1 {
			Debug(rf, dInfo, "%+v", reply)
			if reply.XLen != -1 {
				rf.nextIndex[idx] = reply.XLen // send is short
			} else {
				if reply.XTerm != -1 && rf.Log.contain(reply.XTerm) {
					rf.nextIndex[idx] = rf.Log.TermLastIndex(reply.XTerm)
				} else {
					rf.nextIndex[idx] = reply.XIndex
				}
			}
		}
		Debug(rf, dWarn, " retry to send Log to %s next: %d start: %d", getServerName(idx), rf.nextIndex[idx], rf.Log.start())

		if rf.nextIndex[idx] < rf.Log.start() {
			Debug(rf, dSnap, "%s next: %d start: %d", getServerName(idx), rf.nextIndex[idx], rf.Log.start())
			rf.sendSnapshotLG(idx)
		} else {
			rf.SendLogLG(idx, rf.Log.cloneRange(rf.nextIndex[idx], rf.Log.lastLogIndex())) //
		}
		rf.signLogEnter()
		return
	}

	rf.updateCommitIndexL()
}

// 更新commitIndex
func (rf *Raft) updateCommitIndexL() {
	panicIf(rf.state != Leader, "")
	prev := rf.CommitIndex // for debug
	start := max(rf.CommitIndex+1, rf.Log.start())

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
		// 假设存在 N 满足N > CommitIndex，使得大多数的 matchIndex[i] ≥ N以及log[N].term == CurrentTerm 成立，则令 CommitIndex = N（5.3 和 5.4 节）
		if count > len(rf.peers)/2 {
			rf.CommitIndex = index
		}
	}
	Debug(rf, DIndex, "update commit index; %d -> %d", prev, rf.CommitIndex)
	rf.signLogEnter()
}
