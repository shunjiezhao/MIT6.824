package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"

	//	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type EmptyLog struct {
}

// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (m ApplyMsg) String() string {
	if m.CommandValid {
		return fmt.Sprintf("ApplyMsg{CommandValid: %v, Command: %v, CommandIndex: %v}", m.CommandValid, m.Command, m.CommandIndex)
	} else {
		return fmt.Sprintf("ApplyMsg{SnapshotValid: %v, Snapshot: %v, SnapshotTerm: %v, SnapshotIndex: %v}", m.SnapshotValid, m.Snapshot, m.SnapshotTerm, m.SnapshotIndex)
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	//timeTicker *time.Ticker // 计时器
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//latest term server has seen (initialized to 0  on first boot, increases monotonically)
	// need to stable store
	CurrentTerm int
	VotedFor    int // VotedFor candidateId that received vote in current term (or null if none)
	Log         Log // Log
	logCond     *sync.Cond

	state        state
	electionTime time.Time

	CommitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	name    string // for debug
	applyCh chan ApplyMsg

	LastIncludedTerm, LastIncludedIndex int
	LeaderId                            int
}
type SnapShotInfo struct {
	LastIncludedTerm, LastIncludedIndex int
	Snapshot                            []byte
}
type state uint8

const (
	Follower = iota
	Candidate
	Leader
	heartTime    = time.Millisecond * 150
	heartTimeOut = time.Second
)

func (s state) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("")
	}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	local := getLocal("get state")
	rf.Lock(local)
	defer rf.Unlock(local)
	return rf.CurrentTerm, rf.state == Leader
}

func (rf *Raft) GetLeader() int {
	local := getLocal("get leader")
	rf.Lock(local)
	defer rf.Unlock(local)
	if rf.state == Leader {
		return rf.me
	}
	return rf.LeaderId
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.Save(rf.getRaftState(), rf.persister.ReadSnapshot())
	Debug(rf, dPersist, "persist success!")
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil {
		log.Println(e.Encode(rf.Log))
		panic("decode error")
	}
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.installRaftState(data)
	rf.refreshElectionTime()
	Debug(rf, dTest, "repersist success!")
}

func (rf *Raft) installRaftState(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		CurrentTerm, LastLogIndex, LastLogTerm int
		log                                    Log
		VoteFor                                int
	)
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&LastLogIndex) != nil ||
		d.Decode(&LastLogTerm) != nil ||
		d.Decode(&VoteFor) != nil ||
		d.Decode(&log) != nil {
		panic("decode error")
	}
	rf.CurrentTerm = CurrentTerm
	rf.LastIncludedIndex = LastLogIndex
	rf.LastIncludedTerm = LastLogTerm
	rf.VotedFor = VoteFor
	rf.Log = log
	Debug(rf, dInfo, "start: %d last Index: %d Term: %d", rf.Log.Start, rf.LastIncludedTerm, rf.LastIncludedTerm)
	//rf.Log.Start = rf.LastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	local := getLocal("snapshot")
	rf.Lock(local)
	Debug(rf, dSnap, "%d vs %d", index, rf.LastIncludedIndex)
	if index <= rf.LastIncludedIndex {
		rf.Unlock(local)
		return
	}
	Debug(rf, dSnap, "update last index %d -> %d", rf.LastIncludedIndex, index)

	term := rf.Log.entryAt(index).Term
	rf.Log.setStart(index, term) // cut log -> [lastIncludeIndex, lastIncludeTerm,end]
	rf.installSnapshotOPL(index, rf.Log.entryAt(index).Term, snapshot)

	Debug(rf, dLog, "set start %v Log snap: %d", index, len(snapshot))
	rf.Unlock(local)
}

// 必须持有锁
func (rf *Raft) curTermLowL(term int) bool {
	if term <= rf.CurrentTerm {
		return false
	}
	rf.becameFollower(term)
	return true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to Start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise Start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an electionL. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	local := getLocal("start")
	rf.Lock(local)
	defer rf.Unlock(local)

	if rf.state != Leader {
		return -1, term, false
	}

	entry := LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}

	index = rf.Log.nextLogIndex()
	panicIf(index != rf.Log.lastLogIndex()+1, "")
	rf.AppendLogL(entry)
	rf.persist()
	rf.AppendMsgL(false)
	// Your code here (2B).
	Debug(rf, DSys, "start command %d", index)
	rf.signLogEnter()
	return index, rf.CurrentTerm, true
}
func (rf *Raft) AppendLogL(log LogEntry) {
	// 做一个 广播， 并且呢通知自己 有东西到来
	log.Index = rf.Log.lastLogIndex() + 1
	rf.Log.append(log)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 返回是否到达 选举超时 时间，必须持有锁
func (rf *Raft) ElectionTimeOut() bool {
	return time.Now().After(rf.electionTime)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader electionL should be started.
		local := getLocal("ticker")
		rf.Lock(local)
		if rf.state == Leader {
			rf.refreshElectionTime()
			rf.AppendMsgL(true)
			rf.Unlock(local)
			time.Sleep(heartTime)
			continue
		}
		// 心跳超时, 或者 当前是选举者的状态
		if rf.ElectionTimeOut() {
			rf.electionL()
		}
		rf.Unlock(local)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) State() string {
	return rf.state.String()
}
func (rf *Raft) Name() string {
	return rf.name
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should Start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	runtime.GOMAXPROCS(8)
	labgob.Register(EmptyLog{})

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.name = getServerName(rf.me)

	{
		// Log init
		rf.Log = mkLog(0, 0)
		rf.logCond = sync.NewCond(&rf.mu)
		rf.applyCh = applyCh
		rf.nextIndex = mkSlice(len(rf.peers), 1)
		rf.matchIndex = mkSlice(len(rf.peers), 1)
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.LeaderId = -1
	rf.state = Follower
	rf.CurrentTerm = 0
	rf.retryElectionRefresh()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapShot(persister.ReadSnapshot())
	rf.lastApplied = rf.LastIncludedIndex
	rf.CommitIndex = rf.LastIncludedIndex

	// Start ticker goroutine to Start elections
	go rf.ticker()
	go rf.apply()

	Debug(rf, DSys, "Make ")
	return rf
}

func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func assert(test bool) {
	if test {
		panic("")
	}
}
func panicIf(test bool, str string) {
	if test {
		panic(str)
	}
}

func (rf *Raft) electionL() {
	Debug(rf, dVote, ":%v  %v Start a new %v election ", rf.State(), time.Now(), rf.CurrentTerm+1)
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.refreshElectionTime()
	rf.SendVoteRequestL()
}

func (rf *Raft) refreshElectionTime() {
	rf.electionTime = time.Now().Add(heartTimeOut + time.Duration(rand.Int63()%int64(heartTimeOut)))
}
func (rf *Raft) retryElectionRefresh() {
	rf.electionTime = time.Now().Add(heartTimeOut/4 + time.Duration(rand.Int63()%int64(heartTimeOut/4)))
}
func mkSlice(n int, i int) []int {
	var ans = make([]int, n)
	for a, _ := range ans {
		ans[a] = i
	}
	return ans
}
func (rf *Raft) becameFollower(term int) {
	rf.state = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.LeaderId = -1
	rf.persist()
}

// 刷新 next idx 以及 match idx
func (rf *Raft) freshNextSliceL() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.Log.nextLogIndex()
		Debug(rf, DIndex, "update %s next idx %d", getServerName(i), rf.Log.nextLogIndex())
		rf.matchIndex[i] = 0
	}
}
func getLocal(s string) string {
	return fmt.Sprintf("%s %v", s, time.Now().UnixNano())
}

func (rf *Raft) apply() {

	for rf.killed() == false {
		Debug(rf, dInfo, "apply msg")
		local := getLocal("apply")
		rf.Lock(local)
		for rf.shouldApplyL() == false {
			rf.logCond.Wait()
		}
		panicIf(rf.shouldApplyL() == false, "")

		var msg ApplyMsg
		if rf.lastApplied < rf.LastIncludedIndex {
			rf.lastApplied = rf.LastIncludedIndex
			msg.SnapshotIndex = rf.LastIncludedIndex
			msg.SnapshotTerm = rf.LastIncludedTerm
			msg.SnapshotValid = true
			msg.Snapshot = rf.persister.ReadSnapshot()

		} else if rf.lastApplied < rf.CommitIndex {

			rf.lastApplied++ // 防止重复更新
			msg.CommandValid = true
			msg.Command = rf.Log.entryAt(rf.lastApplied).Command
			msg.CommandIndex = rf.lastApplied
		} else {
		}
		appIdx := rf.lastApplied
		Debug(rf, dClient, "want to apply idx %d Log %+v", rf.lastApplied, rf.Log.entryAt(rf.lastApplied))
		rf.Unlock(local)
		if _, ok := msg.Command.(EmptyLog); ok {
			Debug(rf, dLog, "empty log", appIdx)
			continue
		}
		rf.applyCh <- msg
		Debug(rf, dClient, "apply idx %d Log success", appIdx)
	}
}

func (rf *Raft) shouldApplyL() bool {
	panicIf(rf.lastApplied > rf.CommitIndex, "")
	Debug(rf, dTest, "should apply last: %d commit: %d", rf.lastApplied, rf.CommitIndex)
	if rf.lastApplied >= rf.CommitIndex { // 当提交的都已经被应用了
		//rf.lastApplied = rf.CommitIndex // 可能commit index 更新了
		return false
	}
	// start and last index
	return true
}

func (rf *Raft) signLogEnter() {
	rf.logCond.Broadcast()
}

func (rf *Raft) readSnapShot(snapshot []byte) {
	rf.persister.Save(rf.getRaftState(), snapshot)
}

func (rf *Raft) Lock(local string) {
	Debug(rf, dLock, "Lock %s", local)
	rf.mu.Lock()
	Debug(rf, dLock, "Lock %s success", local)

}

func (rf *Raft) Unlock(local string) {
	Debug(rf, dLock, "Unlock %s", local)
	rf.mu.Unlock()
}
