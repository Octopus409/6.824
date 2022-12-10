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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	// 其他一些包
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// 下面是一些Pretty Debug的定义
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var alreadyInit bool // 是否首次初始化日志参数，全局变量默认为false

func Init() {
	if alreadyInit {
		return
	}
	alreadyInit = true
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug == true {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	Follower = iota
	Candidate
	Leader
)

func min(v1 int, v2 int) int {
	if v1 < v2 {
		return v1
	}
	return v2
}

func max(v1 int, v2 int) int {
	if v1 > v2 {
		return v1
	}
	return v2
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term     int
	Command  interface{}
	LogIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // applier的条件变量

	// 根据论文，给raft设置state
	currentTerm int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	currentState        int   // 当前状态
	election_timeout    int64 // 选举超时时间
	heartsbeats_timeout int64 // 心跳超时时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == Leader {
		isleader = true
	}
	return rf.currentTerm, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveRaftState(rf.encodeState())

	//Debug(dPersist,"S%d Saved State T:%d VF:%d N:%d",rf.me,rf.currentTerm,rf.votedFor,len(rf.log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		Debug(dPersist, "S%d Starting T:%d VF:%d N:%d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(dError, "S%d readPersist error", rf.me)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastApplied = log[0].LogIndex
		rf.commitIndex = log[0].LogIndex
	}
	Debug(dPersist, "S%d Starting T:%d VF:%d N:%d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

// 消除切片数组的底层引用，允许GC回收
func (rf *Raft) shrinkEntries(Entries []Log) []Log {
	return append([]Log{}, Entries...)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().LogIndex
	if snapshotIndex >= index {
		Debug(dSnap, "S%d Rejects the Snapshot as snapshotIndex %d >= index %d", rf.me, snapshotIndex, index)
		return
	}
	rf.log = rf.shrinkEntries(rf.log[index-snapshotIndex:])
	rf.log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	Debug(dSnap, "S%d Accept the Snapshot %d -> %d", rf.me, snapshotIndex, index)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int // 候选人id
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool // 是否投你
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool // true代表同步成功
	IndexNear int  // 用于快速同步nextIndex
}

// InstallSnapShotRPC
type InstallSnapShotArgs struct {
	// 不用分片传的方法
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) getFirstLog() Log {
	return rf.log[0]
}

func (rf *Raft) getLastLog() Log {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) resetElectionTime() {
	// 逻辑外已加锁
	rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300, 150)
}

func (rf *Raft) increaseTerm(NewTerm int) {
	// 升Term、重置votedFor、变为Follower
	Debug(dTerm, "S%d Term update T%d -> T%d", rf.me, rf.currentTerm, NewTerm)
	rf.currentTerm = NewTerm
	rf.votedFor = -1
	if rf.currentState == Leader {
		Debug(dInfo, "S%d Leader -> Follower at T%d", rf.me, rf.currentTerm)
	} else if rf.currentState == Candidate {
		Debug(dInfo, "S%d Canditate -> Follower at T%d", rf.me, rf.currentTerm)
	}
	rf.currentState = Follower
}

func (rf *Raft) isLogUpToDate(LastLogTerm int, LastLogIndex int) bool {
	myLastLogIndex := rf.getLastLog().LogIndex
	myLastLogTerm := rf.getLastLog().Term
	if LastLogTerm > myLastLogTerm {
		return true
	} else if LastLogTerm == myLastLogTerm && LastLogIndex >= myLastLogIndex {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Debug(dVote,"S%d RequestVote from S%d",rf.me,args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		defer rf.persist()
		if rf.currentState == Candidate || rf.currentState == Leader {
			// 同Term，Candidate和Leader不投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
				// 投票成功才重置electionTiemout
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
				defer rf.resetElectionTime()
			} else {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}

	} else if args.Term > rf.currentTerm {
		defer rf.persist()

		rf.increaseTerm(args.Term)

		if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
			// 投票成功才重置electionTiemout
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
			defer rf.resetElectionTime()
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}

	}
}

func (rf *Raft) isLogMatch(PreLogTerm int, PreLogIndex int) bool {
	lastLogIndex := rf.getLastLog().LogIndex
	firstlog := rf.getFirstLog().LogIndex
	if lastLogIndex >= PreLogIndex && PreLogIndex >= firstlog && rf.log[PreLogIndex-firstlog].Term == PreLogTerm {
		return true
	}
	return false
}

func (rf *Raft) updateLog(PreLogIndex int, log []Log, leader int) {
	DeletedNum := 0
	ModifiedNum := 0
	AddNum := 0

	// 遵循不截断原则，除非出现不同，否则不截断后面的。
	// 更新日志，rightLimit是原log最大的下标，超过其就需要用append，如果没超过rightLimit就简单赋值。
	// 此外，还要判断是否存在日志 log[i].Term != rf.log[i].Term ，存在则代表，i后面所有日志都是错误的，需要丢弃。
	// 这样操作，符合截断原则
	firstlog := rf.getFirstLog().LogIndex
	rightLimit := rf.getLastLog().LogIndex
	isDif := false
	for i, l := range log {
		if (PreLogIndex + i + 1) > rightLimit {
			rf.log = append(rf.log, l)
			AddNum++
		} else {
			if rf.log[PreLogIndex+i+1-firstlog].Term != l.Term {
				// 有不同的日志，需要截断PreLogIndex + len(log) 后面的所有日志
				isDif = true
				ModifiedNum++
			}
			rf.log[PreLogIndex+i+1-firstlog] = l
		}
	}
	if isDif && rightLimit > (PreLogIndex+len(log)) {
		rf.log = rf.log[:PreLogIndex+len(log)-firstlog+1]
		DeletedNum = rightLimit - (PreLogIndex + len(log))
	}

	if AddNum > 0 || ModifiedNum > 0 || DeletedNum > 0 {
		// 只有日志发生变化，才打印Log2
		Debug(dLog2, "S%d <- S%d Log Write Add:%d Mod:%d Del:%d", rf.me, leader, AddNum, ModifiedNum, DeletedNum)
	}
}

// 返回PreLogIndex之前（包括PreLogIndex)的某一段Term内的首个日志的index
// 注意判断snapshot的部分
func (rf *Raft) findIndexOfThisTerm(PreLogIndex int) int {
	NearTerm := -1 //日志中PreLogIndex之前的Term
	res := 1
	lastlog := rf.getLastLog().LogIndex
	firstlog := rf.getFirstLog().LogIndex
	if PreLogIndex > lastlog {
		NearTerm = rf.log[len(rf.log)-1].Term
		res = lastlog + 1
		// res最小取值是 firstlog + 1
		for res > firstlog+1 && rf.log[res-firstlog-1].Term == NearTerm {
			res--
		}
	} else {
		NearTerm = rf.log[PreLogIndex-firstlog].Term
		res = PreLogIndex
		for res > firstlog+1 && rf.log[res-firstlog-1].Term == NearTerm {
			res--
		}
	}

	return res

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf(" S%d AppendEntries from S%d",rf.me,args.LeaderId)
	if args.Term < rf.currentTerm {
		// 礼貌回复一下就行
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.IndexNear = -1
	} else if args.Term == rf.currentTerm {
		defer rf.persist()
		defer rf.resetElectionTime()
		// 保持这个Term、重置选举时间
		// Follower要保持Follower，Candidate要降级为Follower，Leader应该是不可能收到这个的
		// ===================日志对比
		if rf.isLogMatch(args.PreLogTerm, args.PreLogIndex) {
			// 匹配的话，要进行日志更新。
			rf.updateLog(args.PreLogIndex, args.Entries, args.LeaderId)
			if args.LeaderCommit > rf.commitIndex {
				// 注意：为防止历史RPC对commitIndex的影响，commitIndex只能增大不能变小
				OldCommitIndex := rf.commitIndex
				tmpCommitIndex := min(args.LeaderCommit, rf.getLastLog().LogIndex)
				rf.commitIndex = max(rf.commitIndex, tmpCommitIndex)
				if OldCommitIndex < rf.commitIndex {
					Debug(dCommit, "S%d Commit Log %d to %d at T%d", rf.me, OldCommitIndex, rf.commitIndex, rf.currentTerm)
				}
				rf.applyCond.Signal()
			}
			reply.Term = rf.currentTerm
			reply.Success = true
			reply.IndexNear = -1
		} else {
			// 不匹配，返回false，等一下轮更新
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.IndexNear = rf.findIndexOfThisTerm(args.PreLogIndex)
		}

		if rf.currentState == Candidate {
			rf.currentState = Follower
			Debug(dInfo, "S%d Canditate -> Follower at T%d", rf.me, rf.currentTerm)
		}

	} else if args.Term > rf.currentTerm {
		defer rf.persist()
		defer rf.resetElectionTime()
		// 进入新Term、设置voteFor、重置选举时间
		// Follower保持Follower，其他降级为Follower

		rf.increaseTerm(args.Term)

		// 判断日志，依据返回True和False
		// ===================日志对比
		if rf.isLogMatch(args.PreLogTerm, args.PreLogIndex) {
			// 匹配的话，要进行日志更新。
			//DPrintf("[调试] S%d Log match with S%d",rf.me,args.LeaderId)
			rf.updateLog(args.PreLogIndex, args.Entries, args.LeaderId)
			if args.LeaderCommit > rf.commitIndex {
				OldCommitIndex := rf.commitIndex
				tmpCommitIndex := min(args.LeaderCommit, rf.getLastLog().LogIndex)
				rf.commitIndex = max(rf.commitIndex, tmpCommitIndex)
				if OldCommitIndex < rf.commitIndex {
					Debug(dCommit, "S%d Commit Log %d to %d at T%d", rf.me, OldCommitIndex, rf.commitIndex, rf.currentTerm)
				}
				//DPrintf("[调试][argsTerm大于currentTerm] S%d update commitIndex (from %d to %d) because %d, and lastapplied is %d",rf.me,OldCommitIndex,rf.commitIndex,args.LeaderId,rf.lastApplied)
				rf.applyCond.Signal()
			}
			reply.Term = rf.currentTerm
			reply.Success = true
			reply.IndexNear = -1
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.IndexNear = rf.findIndexOfThisTerm(args.PreLogIndex)
		}

	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.increaseTerm(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	defer rf.resetElectionTime()

	firstlog := rf.getFirstLog().LogIndex
	lastlog := rf.getLastLog().LogIndex
	if args.LastIncludedIndex <= rf.commitIndex {
		// 没有新的日志信息
		Debug(dSnap, "S%d Rejects the InstallSnapShotRPC from S%d since LastIncludedIndex %d is less than commitIndex %d", rf.me, args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
		return
	}
	if args.LastIncludedIndex >= lastlog {
		// 清空所有日志，只留记录snapshot的空日志
		rf.log = rf.shrinkEntries(rf.log[0:1])
		rf.log[0].LogIndex = args.LastIncludedIndex
		rf.log[0].Term = args.LastIncludedTerm
		rf.log[0].Command = nil
	} else {
		rf.log = rf.shrinkEntries(rf.log[args.LastIncludedIndex-firstlog:])
		if rf.log[0].Term != args.LastIncludedTerm {
			// 日志有冲突，discard LastIncludedIndex 后面所有日志
			OldLen := len(rf.log)
			rf.log = rf.shrinkEntries(rf.log[0:1])
			Debug(dSnap, "S%d Log Contradiction discard %d Logs after index %d", rf.me, OldLen-1, rf.log[0].LogIndex)
		}
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  rf.log[0].Term,
			SnapshotIndex: rf.log[0].LogIndex,
		}
	}()
	OldCommitIndex := rf.commitIndex
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	Debug(dSnap, "S%d InstallSnapShot CommitIndex %d -> %d datalen %d", rf.me, OldCommitIndex, rf.commitIndex, len(args.Data))
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// RequestVote函数，保证必定返回。
	// 如果rpc成功，有返回值，  True
	// 如果rpc在一定时间内没有反应， False
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.currentState != Leader {
		return -1, -1, false
	}
	index = rf.getLastLog().LogIndex + 1
	logEntry := Log{
		Term:     rf.currentTerm,
		Command:  command,
		LogIndex: index,
	}
	rf.log = append(rf.log, logEntry)

	term = rf.currentTerm

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// 每10毫秒检测一次选举超时
		time.Sleep(20 * time.Millisecond)
		//fmt.Println("Now time: ",time.Now().Format("15:04:05.000"))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()

		if rf.currentState != Leader && time.Now().UnixMilli() >= rf.election_timeout {
			// 发动选举
			// 把操作封装在一个函数

			rf.begin_eletion()

		} else if rf.currentState == Leader && time.Now().UnixMilli() >= rf.heartsbeats_timeout {

			// 发起心跳
			rf.begin_heartsbeats()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	// 不加锁，默认调用这个的时候，已经加锁
	res := RequestVoteArgs{}
	res.Term = rf.currentTerm
	res.CandidateId = rf.me
	res.LastLogIndex = rf.getLastLog().LogIndex
	res.LastLogTerm = rf.getLastLog().Term
	return &res
}

func (rf *Raft) genAppendEntriesArgs(peer int) *AppendEntriesArgs {
	// 不加锁，默认调用这个的时候，已经加锁
	res := AppendEntriesArgs{}
	res.Term = rf.currentTerm
	res.LeaderId = rf.me
	// Log[]、PreLogIndex、PreLogTerm关联于不同发送对象
	lastlog := rf.getLastLog().LogIndex
	firstlog := rf.getFirstLog().LogIndex
	if lastlog >= rf.nextIndex[peer] {
		// 有log要发送
		for i := rf.nextIndex[peer]; i <= lastlog; i++ {
			res.Entries = append(res.Entries, rf.log[i-firstlog])
		}
	}
	res.PreLogIndex = rf.nextIndex[peer] - 1
	res.PreLogTerm = rf.log[res.PreLogIndex-firstlog].Term

	res.LeaderCommit = rf.commitIndex

	return &res
}

func (rf *Raft) genInstallSnapShotArgs(peer int) *InstallSnapShotArgs {
	res := InstallSnapShotArgs{
		// 不用分片传的方法
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].LogIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return &res
}

func (rf *Raft) begin_eletion() {

	rf.currentState = Candidate
	rf.currentTerm++ // 增加term
	defer rf.resetElectionTime()

	Debug(dTimer, "S%d Begin Election", rf.me)
	if rf.currentState == Candidate {
		Debug(dInfo, "S%d Follower -> Candidate", rf.me) // 如果已经是candidate，就不用再次打印此日志
	}
	Debug(dTerm, "S%d Term update T%d -> T%d", rf.me, rf.currentTerm-1, rf.currentTerm)

	// 使用协程发动选举
	// 需要对candidate当前状态进行一个快照，也就是requestVote的args要保存
	grantedVote := 1
	rf.votedFor = rf.me
	args := rf.genRequestVoteArgs()
	rf.persist()

	validTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.one_RequestVote(peer, args, &grantedVote, validTerm)
	}
}

func (rf *Raft) one_RequestVote(peer int, args *RequestVoteArgs, grantedVote *int, validTerm int) {
	// 创建发送args和reply的数组
	response := RequestVoteReply{}
	if rf.sendRequestVote(peer, args, &response) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		if rf.currentTerm == response.Term && rf.currentTerm == validTerm && response.VoteGranted {
			// 对面投票了，说明他的Term小于等于我的
			*grantedVote++
			Debug(dVote, "S%d Got Vote <- S%d", rf.me, peer)
			if *grantedVote > len(rf.peers)/2 && rf.currentState != Leader {
				rf.currentState = Leader
				// 成为leader，不需要管election_timeout，这个等下一次变为follower时再更新
				// 成为leader，要初始化nextIndex[]和matchIndex[]
				lastlog := rf.getLastLog().LogIndex
				for peer := range rf.peers {
					rf.nextIndex[peer] = lastlog + 1
					rf.matchIndex[peer] = 0
				}

				// 开启发送心跳报文
				rf.heartsbeats_timeout = time.Now().UnixMilli()
				Debug(dLeader, "S%d Achieved Majority for T%d , converting to Leader", rf.me, rf.currentTerm)
				//rf.begin_heartsbeats()
			}
		} else if response.Term > rf.currentTerm {
			rf.increaseTerm(response.Term)
		}
	}
	// 只发送一次，发送失败就算了
}

func (rf *Raft) begin_heartsbeats() {

	rf.heartsbeats_timeout = time.Now().UnixMilli() + get_rand_time(100, 0)
	Debug(dTimer, "S%d Begin heartsbeats LogLen %d, LastLogIndex %d, SnapShotIndex %d", rf.me, len(rf.log), rf.getLastLog().LogIndex, rf.log[0].LogIndex)

	firstlog := rf.getFirstLog().LogIndex
	lastlog := rf.getLastLog().LogIndex
	validTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.nextIndex[peer] > firstlog {
			// 日志尚在，正常发送
			args := rf.genAppendEntriesArgs(peer)
			go rf.one_AppendEntries(peer, args, lastlog, validTerm)
		} else {
			// 日志已被压缩，要InstallSnapShotRPC
			args := rf.genInstallSnapShotArgs(peer)
			go rf.one_InstallSnapShot(peer, args, validTerm)
		}
	}

}

func (rf *Raft) one_AppendEntries(peer int, args *AppendEntriesArgs, lastlog int, validTerm int) {
	reply := AppendEntriesReply{}
	Debug(dLog, "S%d -> S%d Sending AppendEntries PLI:%d PLT:%d N:%d LC:%d", rf.me, peer, args.PreLogIndex, args.PreLogTerm, len(args.Entries), args.LeaderCommit)
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//if validTerm!=rf.currentTerm || rf.currentState!=Leader {
		//	// server已经不是leader，直接跳出
		//	// 或者并非validTerm，是历史RPC的reply可以直接退出。
		//	return
		//}
		defer rf.persist()
		firstlog := rf.getFirstLog().LogIndex
		if reply.Term > rf.currentTerm {
			rf.increaseTerm(reply.Term)
		} else if rf.currentTerm == validTerm && rf.currentTerm == reply.Term && reply.Success {
			// 同步成功
			// 写成这样是为了考虑历史RPC的影响，导致matchIndex出现倒退的问题。
			if rf.nextIndex[peer] < lastlog+1 {
				rf.nextIndex[peer] = lastlog + 1
			}
			if rf.matchIndex[peer] < lastlog {
				rf.matchIndex[peer] = lastlog
			}
			Debug(dLog, "S%d <- S%d OK Append NI[%d]=%d, MI[%d]=%d", rf.me, peer, peer, rf.nextIndex[peer], peer, rf.matchIndex[peer])

			// 检查commitIndex能否更新

			// 这里要判断是否是当前Term的log，否则不予commit
			if (lastlog > rf.commitIndex) && (rf.log[lastlog-firstlog].Term == rf.currentTerm) && rf.canIncreaseCommitIndex(lastlog) {
				//fmt.Println("increasing the commitIndex")
				OldCommitIndex := rf.commitIndex
				rf.commitIndex = lastlog
				Debug(dCommit, "S%d Commit Log %d to %d at T%d", rf.me, OldCommitIndex, rf.commitIndex, rf.currentTerm)
				rf.applyCond.Signal()
			}
		} else if rf.currentTerm == validTerm && rf.currentTerm == reply.Term && !reply.Success {
			// 日志匹配失败，降低nextIndex至reply中提示的IndexNear
			rf.nextIndex[peer] = min(reply.IndexNear, rf.nextIndex[peer]) // 防一手历史RPC，nextIndex只能降低不能升高
			Debug(dLog, "S%d <- S%d Not match Append NI[%d]=%d", rf.me, peer, peer, rf.nextIndex[peer])
		}
	}
	// 发送失败同样不处理，等下一次心跳
}

func (rf *Raft) one_InstallSnapShot(peer int, args *InstallSnapShotArgs, validTerm int) {
	reply := InstallSnapShotReply{}
	Debug(dSnap, "S%d -> S%d Sending InstallSnapShot LII:%d LIT:%d dataLen:%d", rf.me, peer, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
	if rf.sendInstallSnapShot(peer, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		if reply.Term > rf.currentTerm {
			rf.increaseTerm(reply.Term)
			return
		}
		if rf.currentTerm == validTerm {
			// install successfully
			rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
			Debug(dSnap, "S%d <- S%d OK Snapshot NI[%d]=%d, MI[%d]=%d", rf.me, peer, peer, rf.nextIndex[peer], peer, rf.matchIndex[peer])
		}
	}
}

// applier的后台协程，只有一个
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			//fmt.Println("Waiting")
			rf.applyCond.Wait()
		}

		// 判断commitIndex小于firstlogIndex?

		//fmt.Println("Ready to commit")
		firstlog := rf.getFirstLog().LogIndex
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Log, commitIndex-lastApplied)
		//fmt.Println("commiting ",commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-firstlog:commitIndex+1-firstlog])

		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.LogIndex,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback

		rf.lastApplied = max(rf.lastApplied, commitIndex)
		Debug(dCommit, "S%d Applied Log %d to %d", rf.me, lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) canIncreaseCommitIndex(index int) bool {
	//fmt.Println("Checking weather can increase the commitIndex")
	sum := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.matchIndex[peer] >= index {
			sum++
		}
	}
	return sum > len(rf.peers)/2
}

func get_rand_time(base int, ran int) int64 {

	if ran == 0 {
		return int64(base)
	}

	return int64(base + rand.Int()%ran)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.dead = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.currentState = Follower
	rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300, 150)

	// 每个server填充一个空的log，这个空log为了index计算方便。可以认为是一个空的snapshot
	empty_log := Log{
		Term:     0,
		Command:  nil,
		LogIndex: 0,
	}
	rf.log = append(rf.log, empty_log)

	// 初始化日志参数
	Init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
