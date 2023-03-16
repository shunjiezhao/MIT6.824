package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate term
	CandidateId  int // candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)

	Name string
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// 必须持有锁
func (rf *Raft) curTermLow(term int) bool {
	if term <= rf.currentTerm {
		return false
	}

	rf.state = Follower
	rf.currentTerm = term
	return true
}

// example RequestVote RPC handler.
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
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
	if rf.curTermLow(args.Term) {
		rf.votedFor = -1
		Debug(rf, dError, "%s 过期", rf.Name())
	}

	//这轮 已经投过别人了
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	Debug(rf, dVote, "%s[%s,%d]< %s[%d]", rf.Name(), rf.State(), rf.currentTerm, getServerName(args.CandidateId),
		args.Term)

	rf.votedFor = args.CandidateId
	Debug(rf, dVote, "%s[%s] 投票给 %s: success %+v %v", rf.Name(), rf.State(), getServerName(args.CandidateId), reply)
	rf.state = Candidate
	reply.VoteGranted = true
	rf.refreshElectionTime()
	return
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm

	isLeader := rf.state == Leader
	// Your code here (2B).
	Debug(rf, dInfo, "%s Start %d %v", rf.Name(), term, isLeader)
	return index, term, isLeader
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
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.leaderOp() // 会自己释放锁
			time.Sleep(heartTime)
			continue
		}
		// 心跳超时, 或者 当前是选举者的状态
		if rf.ElectionTimeOut() {
			rf.election()
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
	return getServerName(rf.me)
}
func (rf *Raft) leaderOp() {
	term := rf.currentTerm
	name := rf.Name()
	state := rf.State()
	rf.mu.Unlock()

	//Debug(rfdLeader, "%s Term: %d send heartbeat", rf.Name(), term)
	wg := &sync.WaitGroup{}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go rf.appendMsg(wg, name, true, state, term, i)
	}
}

func (rf *Raft) appendMsg(wg *sync.WaitGroup, name string, heart bool, state string, term int, idx int) {
	defer wg.Done()
	var (
		args  AppendEntriesArgs
		reply AppendEntriesReply
	)
	args.Term = term
	args.LeaderId = rf.me
	call := rf.appendEntries(idx, &args, &reply)
	Debug(rf, dLeader, "%s[%s] -> %s call:%v req: %+v ans %+v", name, state, getServerName(idx), call, args, reply)
	if !call && (reply.Success == true || reply.Term != 0) {

		panic("Leader: rpc call failed but get ans")
	}
	rf.mu.Lock()
	if rf.curTermLow(reply.Term) {
		// 如果一个 candidate 或者 leader 发现自己的任期号过期了，它就会立刻回到 follower 状态。
	}
	rf.mu.Unlock()
	return

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	runtime.GOMAXPROCS(8)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.refreshElectionTime()

	//rf.timeTicker = time.NewTicker(getRandTime())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	Debug(rf, dInfo, "%s Make ", rf.Name())
	return rf
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int // leader’s term
	LeaderId int // so follower can redirect clients
	//prevLogIndex int // index of log entry immediately preceding new ones
	//prevLogTerm  int //term of prevLogIndex entry
	//entries[]        // log entries to store (empty for heartbeat may send more than one for efficiency)
	//leaderCommit int // leader’s commitIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.timeTicker.Reset(getRandTime())
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		//如果一个节点接收了一个带着过期的任期号的请求，那么它会拒绝这次请求。
		// 如果这个 leader 的任期号小于这个 candidate 的当前任期号，那么这个 candidate 就会拒绝这次 RPC，然后继续保持 candidate 状态。
		return
	}

	// 所有服务器遵守的规则
	reply.Success = true
	Debug(rf, dTimer, "%s[%s]<- %s [term: %d] ", rf.Name(), rf.State(), getServerName(args.LeaderId), args.Term)
	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm = args.Term
		return
	}
	//如果这个 leader 的任期号（这个任期号会在这次 RPC 中携带着）不小于这个 candidate 的当前任期号，那么这个 candidate 就会觉得这个 leader 是合法的，然后将自己转变为 follower 状态。
	// 当前任期 curTerm = args.Term
	rf.state = Follower
	rf.refreshElectionTime() // 更新心跳时间
	return
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

func (rf *Raft) election() {
	rf.state = Candidate
	rf.currentTerm++
	Debug(rf, dTimer, "%s:%v  %v start a new %v election ", rf.Name(), rf.State(), time.Now(), rf.currentTerm+1)
	rf.votedFor = rf.me
	term := rf.currentTerm
	rf.refreshElectionTime()
	count := 1
	go func() {
		wg := &sync.WaitGroup{}
		// 通知
		for other, _ := range rf.peers {
			if other == rf.me {
				continue
			}
			wg.Add(1)
			go rf.sendVoteRequest(wg, &count, other, term)
		}
		wg.Wait()
		rf.mu.Lock()
		if rf.state == Candidate {
			rf.electionTime = time.Now() // 再次选举
			Debug(rf, dError, "%s 选举失败 retry selection", rf.Name())
		}
		rf.mu.Unlock()
	}()

}

func (rf *Raft) sendVoteRequest(wg *sync.WaitGroup, count *int, other int, curTerm int) {
	defer wg.Done()
	var (
		req   RequestVoteArgs
		reply RequestVoteReply
	)

	req.Term = curTerm
	req.CandidateId = rf.me
	call := rf.sendRequestVote(other, &req, &reply)
	Debug(rf, dVote, "%s -> % s[call:%t] req: %+v ans: %+v", rf.Name(), getServerName(other), call, req, reply)

	assert(!call && reply.VoteGranted == true)

	rf.mu.Lock()
	if rf.curTermLow(reply.Term) {

	} else {
		if reply.VoteGranted == false {
			Debug(rf, dError, "%s is 没有获取到 %s 票", rf.Name(), getServerName(other))
		} else {
			Debug(rf, dVote, "%s is 获取到 %s 票 count:%v", rf.Name(), getServerName(other), *count)
			*count++
			if *count > len(rf.peers)/2 && rf.state == Candidate {
				Debug(rf, dVote, "%s now is leader", rf.Name())
				rf.state = Leader
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) refreshElectionTime() {
	rf.electionTime = time.Now().Add(heartTimeOut)
}
