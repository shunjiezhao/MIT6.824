package raft

import (
	"fmt"
	"time"
)

type InstallSnapshotReq struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int // 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int // 快照中包含的最后日志条目的任期号
	Data              []byte
}

type InstallSnapshotResp struct {
	Term int
}

func (a InstallSnapshotReq) String() string {
	return fmt.Sprintf("InstallSnapshotReq: term:%d "+
		"LeaderId:%d LastIncludedIndex: %d LastIncludedTerm:%d", a.Term, a.LeaderID, a.LastIncludedIndex,
		a.LastIncludedTerm)
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotReq, reply *InstallSnapshotResp) {

	local := fmt.Sprintf("snapshot %v", time.Now())
	rf.Lock(local)
	defer rf.Unlock(local)
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm { // rule 1
		Debug(rf, dWarn, "found %d 过期", getServerName(args.LeaderID))
		return
	}
	if rf.curTermLowL(args.Term) {
		reply.Term = rf.CurrentTerm
		Debug(rf, dWarn, "过期 在 install snap")
	}
	if args.LastIncludedIndex <= rf.LastIncludedIndex { // == 说明已经安装过了
		Debug(rf, dWarn, "已经安装过了")
		return
	}

	Debug(rf, dSnap, "install snapshot")
	if rf.Log.start() <= args.LastIncludedIndex && rf.Log.lastLogIndex() >= args.LastIncludedIndex &&
		rf.Log.entryAt(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.Log.setStart(args.LastIncludedIndex, args.LastIncludedTerm) // cut log -> [lastIncludeIndex, lastIncludeTerm,end]
	} else {
		rf.Log = mkLog(args.LastIncludedIndex, args.LastIncludedTerm) // 截断
	}
	rf.installSnapshotOPL(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	// send snapshot to config.go
	Debug(rf, dSnap, "apply snapshot index: %d term: %d success", args.LastIncludedTerm, args.LastIncludedIndex)
	return
}

func (rf *Raft) sendSnapshotLG(server int) {
	Debug(rf, dSnap, " call install to %s", getServerName(server))
	var args = InstallSnapshotReq{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	Debug(rf, dSnap, "-> %s %s", getServerName(server), args)
	panicIf(len(args.Data) == 0, "snap shot is nil")
	go rf.sendSnapShot(server, &args)
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotReq) {
	var reply InstallSnapshotResp
	Debug(rf, dSnap, "sendSnapShot to %s", getServerName(server))

	call := rf.installSnapshot(server, args, &reply)
	if !call {
		Debug(rf, "-> %s failed", getServerName(server))
		return
	}
	local := fmt.Sprintf("sendSnapShot %v", time.Now().Unix())
	rf.Lock(local)
	defer rf.Unlock(local)
	if rf.state != Leader {
		Debug(rf, dInfo, "is not leader!")
		return
	}
	if rf.curTermLowL(reply.Term) {
		Debug(rf, dWarn, "过期")
		return
	}
	Debug(rf, dSnap, "install snapshot success! lastapplied: %d CommitIndex: %d", rf.lastApplied, rf.CommitIndex)
	rf.procSnapshotReplyL(server, args, &reply)
	return
}

func (rf *Raft) procSnapshotReplyL(server int, args *InstallSnapshotReq, reply *InstallSnapshotResp) {
	prev := rf.nextIndex[server]
	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	Debug(rf, dSnap, "update %s nextIdx: %d -> %d", getServerName(server), prev, rf.nextIndex[server])
	rf.updateCommitIndexL()
}

func (rf *Raft) installSnapshot(server int, args *InstallSnapshotReq, reply *InstallSnapshotResp) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) installSnapshotOPL(lastIncludeIndex, lastIncludeTerm int, snapshot []byte) {
	prevA, prevC := rf.lastApplied, rf.CommitIndex
	rf.LastIncludedIndex = lastIncludeIndex
	rf.LastIncludedTerm = lastIncludeTerm
	rf.CommitIndex = max(lastIncludeIndex, rf.CommitIndex)
	//rf.lastApplied = max(rf.LastIncludedIndex, rf.lastApplied)
	rf.persister.Save(rf.getRaftState(), snapshot)
	Debug(rf, dSnap, "after install snapshot lastlogindex: %d "+
		"last: %d -> %d commit: %d -> %d", rf.Log.lastLogIndex(), prevA, rf.lastApplied, prevC, rf.CommitIndex)
	rf.signLogEnter()
}
