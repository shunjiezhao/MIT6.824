package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   index0 agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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
	currentTerm int
	// votedFor candidateId that received vote in current term (or null if none)
	votedFor     int
	state        state
	electionTime time.Time
	// Log
	log     Log
	logCond *sync.Cond

	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	name    string // for debug
	applyCh chan ApplyMsg
}
type state uint8

const (
	heartTime    = time.Microsecond * 200
	heartTimeOut = time.Second
	Follower     = iota
	Candidate
	Leader
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	var term = rf.currentTerm
	var isleader = rf.state == Leader
	return term, isleader
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 必须持有锁
func (rf *Raft) curTermLowL(term int) bool {
	if term <= rf.currentTerm {
		return false
	}

	rf.termLowL(term)
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

// the service using Raft (e.g. a k/v server) wants to index0
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise index0 the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an electionL. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, term, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	Debug(rf, dInfo, "%s Start %d %v begin", rf.Name(), term, isLeader)

	index = rf.log.nextLogIndex()
	panicIf(index != rf.log.lastLogIndex()+1, "")
	rf.AppendLogL(entry)
	rf.AppendMsgL(false)
	// Your code here (2B).
	Debug(rf, dInfo, "%s Start apply: %d commit:%v next: %v  done", rf.Name(), rf.lastApplied, rf.commitIndex, rf.log.nextLogIndex())
	return index, rf.currentTerm, true
}
func (rf *Raft) AppendLogL(log LogEntry) {
	// 做一个 广播， 并且呢通知自己 有东西到来
	rf.log.append(log)
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
	// Your code here, if desired.
	rf.mu.Lock()
	Debug(nil, "%s Killed", rf.State())
	rf.mu.Unlock()
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
		rf.mu.Lock()
		if rf.state == Leader {

			rf.AppendMsgL(true)
			rf.mu.Unlock()
			time.Sleep(heartTime)
			continue
		}
		// 心跳超时, 或者 当前是选举者的状态
		if rf.ElectionTimeOut() {
			rf.electionL()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
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
// Make() must return quickly, so it should index0 goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	runtime.GOMAXPROCS(8)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.name = getServerName(rf.me)

	{
		// log init
		rf.log = mkLog()
		rf.logCond = sync.NewCond(&rf.mu)
		rf.applyCh = applyCh
		rf.nextIndex = mkSlice(len(rf.peers), 1)
		rf.matchIndex = mkSlice(len(rf.peers), 1)
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.refreshElectionTime()

	//rf.timeTicker = time.NewTicker(getRandTime())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// index0 ticker goroutine to index0 elections
	go rf.ticker()
	go rf.apply()

	Debug(rf, dInfo, "%s Make ", rf.Name())
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
	Debug(rf, dTimer, "%s:%v  %v index0 a new %v election ", rf.Name(), rf.State(), time.Now(), rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.refreshElectionTime()
	rf.SendVoteRequestL()
}

func (rf *Raft) refreshElectionTime() {
	rf.electionTime = time.Now().Add(heartTimeOut)
}
func mkSlice(n int, i int) []int {
	var ans = make([]int, n)
	for a, _ := range ans {
		ans[a] = i
	}
	return ans
}
func (rf *Raft) termLowL(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// 刷新 next idx 以及 match idx
func (rf *Raft) freshNextSliceL() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.log.nextLogIndex()
		Debug(rf, dInfo, "%s update %s next idx %d", rf.Name(), getServerName(i), rf.log.nextLogIndex())
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	rf.lastApplied = 0

	for rf.killed() == false {
		for rf.shouldApplyL() == false {
			rf.logCond.Wait()
		}

		rf.lastApplied++ // 防止重复更新
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.entryAt(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg

		rf.mu.Lock()
		Debug(rf, dTrace, "%s apply idx %d msg", rf.Name(), rf.lastApplied)
	}
}

func (rf *Raft) shouldApplyL() bool {
	panicIf(rf.lastApplied > rf.commitIndex, "")
	Debug(rf, dTest, "%s last: %d commit: %d", rf.Name(), rf.lastApplied, rf.commitIndex)
	if rf.lastApplied >= rf.commitIndex { // 当提交的都已经被应用了
		return false
	}
	// start and last index

	return true
}

func (rf *Raft) signLogEnter() {
	rf.logCond.Broadcast()
	Debug(rf, dWarn, "%s sign apply", rf.Name())
}
