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

	//	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	name    string // for debug
	applyCh chan ApplyMsg
}
type state uint8

const (
	Follower = iota
	Candidate
	Leader
	heartTime    = time.Millisecond * 200
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	var term = rf.CurrentTerm
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.CurrentTerm); err != nil {
		panic(err)
	}

	if err := e.Encode(rf.VotedFor); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.Log); err != nil {
		panic(err)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	Debug(rf, dPersist, "%s persist success!", rf.name)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		CurrentTerm int
		log         Log
		VoteFor     int
	)
	if err := d.Decode(&CurrentTerm); err != nil {
		panic(fmt.Sprintf("decode current term: %v", err))
	}
	rf.CurrentTerm = CurrentTerm

	if err := d.Decode(&VoteFor); err != nil {
		panic(fmt.Sprintf("decode VoteFor: %v", err))
	}
	rf.VotedFor = VoteFor
	if err := d.Decode(&log); err != nil {
		panic(fmt.Sprintf("decode Log: %v", err))
	}
	rf.Log = log
	rf.refreshElectionTime()
	Debug(rf, dTest, "repersist success!")
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 必须持有锁
func (rf *Raft) curTermLowL(term int) bool {
	if term <= rf.CurrentTerm {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	Debug(rf, DSys, "%s start command", rf.name)
	return index, rf.CurrentTerm, true
}
func (rf *Raft) AppendLogL(log LogEntry) {
	// 做一个 广播， 并且呢通知自己 有东西到来
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
	// Your code here, if desired.
	rf.mu.Lock()
	Debug(nil, DSys, "%s Killed", rf.State())
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
			rf.refreshElectionTime()
			rf.AppendMsgL(false)
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
// Make() must return quickly, so it should Start goroutines
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
		// Log init
		rf.Log = mkLog()
		rf.logCond = sync.NewCond(&rf.mu)
		rf.applyCh = applyCh
		rf.nextIndex = mkSlice(len(rf.peers), 1)
		rf.matchIndex = mkSlice(len(rf.peers), 1)
	}
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.state = Follower
	rf.CurrentTerm = 0
	rf.refreshElectionTime()
	rf.lastApplied = 0

	//rf.timeTicker = time.NewTicker(getRandTime())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to Start elections
	go rf.ticker()
	go rf.apply()

	Debug(rf, DSys, "%s Make ", rf.Name())
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
	Debug(rf, dVote, "%s:%v  %v Start a new %v election ", rf.Name(), rf.State(), time.Now(), rf.CurrentTerm+1)
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.refreshElectionTime()
	rf.SendVoteRequestL()
}

func (rf *Raft) refreshElectionTime() {
	rf.electionTime = time.Now().Add(heartTimeOut + time.Duration(rand.Int63()%int64(heartTimeOut)))
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
	rf.CurrentTerm = term
	rf.VotedFor = -1
	//TODO: need to persist
	rf.persist()
}

// 刷新 next idx 以及 match idx
func (rf *Raft) freshNextSliceL() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.Log.nextLogIndex()
		Debug(rf, DIndex, "%s update %s next idx %d", rf.Name(), getServerName(i), rf.Log.nextLogIndex())
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) apply() {

	for rf.killed() == false {
		rf.mu.Lock()
		for rf.shouldApplyL() == false {
			rf.logCond.Wait()
		}
		panicIf(rf.shouldApplyL() == false, "")

		rf.lastApplied++ // 防止重复更新
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log.entryAt(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		Debug(rf, dClient, "%s want to apply idx %d Log %+v", rf.name, rf.lastApplied, rf.Log.entryAt(rf.lastApplied))
		Debug(rf, dLog, "%s apply idx %d msg", rf.Name(), rf.lastApplied)
		rf.mu.Unlock()
		rf.applyCh <- msg

	}
}

func (rf *Raft) shouldApplyL() bool {
	panicIf(rf.lastApplied > rf.commitIndex, "")
	if rf.lastApplied >= rf.commitIndex { // 当提交的都已经被应用了
		//rf.lastApplied = rf.commitIndex // 可能commit index 更新了
		return false
	}
	// start and last index
	Debug(rf, dTest, "%s should apply last: %d commit: %d", rf.Name(), rf.lastApplied, rf.commitIndex)

	return true
}

func (rf *Raft) signLogEnter() {
	rf.logCond.Broadcast()
}
