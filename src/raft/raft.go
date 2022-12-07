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

func Init() {
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
	matcnIndex []int

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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
	}
	Debug(dPersist, "S%d Starting T:%d VF:%d N:%d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
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

func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].LogIndex
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
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[len(rf.log)-1].Term
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
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex >= PreLogIndex && rf.log[PreLogIndex].Term == PreLogTerm {
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
	rightLimit := len(rf.log) - 1
	isDif := false
	for i, l := range log {
		if (PreLogIndex + i + 1) > rightLimit {
			rf.log = append(rf.log, l)
			AddNum++
		} else {
			if rf.log[PreLogIndex+i+1].Term != l.Term {
				// 有不同的日志，需要截断PreLogIndex + len(log) 后面的所有日志
				isDif = true
				ModifiedNum++
			}
			rf.log[PreLogIndex+i+1] = l
		}
	}
	if isDif && rightLimit > (PreLogIndex+len(log)) {
		rf.log = rf.log[:PreLogIndex+len(log)+1]
		DeletedNum = rightLimit - (PreLogIndex + len(log))
	}

	if AddNum > 0 || ModifiedNum > 0 || DeletedNum > 0 {
		// 只有日志发生变化，才打印Log2
		Debug(dLog2, "S%d <- S%d Log Write Add:%d Mod:%d Del:%d", rf.me, leader, AddNum, ModifiedNum, DeletedNum)
	}
}

func (rf *Raft) findIndexOfThisTerm(PreLogIndex int) int {
	NearTerm := -1 //日志中PreLogIndex之前的Term
	res := 1
	if PreLogIndex > len(rf.log)-1 {
		NearTerm = rf.log[len(rf.log)-1].Term
		res = len(rf.log)
		for res > 1 && rf.log[res-1].Term == NearTerm {
			res--
		}
	} else {
		NearTerm = rf.log[PreLogIndex].Term
		res = PreLogIndex
		for res > 1 && rf.log[res-1].Term == NearTerm {
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
				tmpCommitIndex := min(args.LeaderCommit, len(rf.log)-1)
				rf.commitIndex = max(rf.commitIndex, tmpCommitIndex)
				if OldCommitIndex < rf.commitIndex {
					Debug(dCommit, "S%d Commit Log %d to %d at T%d", rf.me, OldCommitIndex, rf.commitIndex, rf.currentTerm)
				}
				//fmt.Println("Im ",rf.me," LeaderCommit is ",args.LeaderCommit," .New CommitIndex is ",rf.commitIndex)
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
				tmpCommitIndex := min(args.LeaderCommit, len(rf.log)-1)
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

	//rf.mu.Lock()

	/*   不管发送前，状态有没有变化，好像还是发送比较好。
	if rf.currentState == Follower || rf.currentState==Leader {
		rf.mu.Unlock()
		return false
	}
	*/
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index = len(rf.log)
	logEntry := Log{
		Term:     rf.currentTerm,
		Command:  command,
		LogIndex: index + 1,
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
	res.LastLogIndex = len(rf.log) - 1
	res.LastLogTerm = rf.log[len(rf.log)-1].Term
	return &res
}

func (rf *Raft) genAppendEntriesArgs(peer int) *AppendEntriesArgs {
	// 不加锁，默认调用这个的时候，已经加锁
	res := AppendEntriesArgs{}
	res.Term = rf.currentTerm
	res.LeaderId = rf.me
	// Log[]、PreLogIndex、PreLogTerm关联于不同发送对象
	if (len(rf.log) - 1) >= rf.nextIndex[peer] {
		// 有log要发送
		for i := rf.nextIndex[peer]; i < len(rf.log); i++ {
			res.Entries = append(res.Entries, rf.log[i])
		}
	}
	res.PreLogIndex = rf.nextIndex[peer] - 1
	res.PreLogTerm = rf.log[rf.nextIndex[peer]-1].Term

	res.LeaderCommit = rf.commitIndex

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
			if *grantedVote > len(rf.peers)/2 {
				rf.currentState = Leader
				// 成为leader，不需要管election_timeout，这个等下一次变为follower时再更新
				// 成为leader，要初始化nextIndex[]和matchIndex[]
				lastlog := len(rf.log) - 1
				for peer := range rf.peers {
					rf.nextIndex[peer] = lastlog + 1
					rf.matcnIndex[peer] = 0
				}

				// 开启发送心跳报文
				rf.heartsbeats_timeout = time.Now().UnixMilli()
				Debug(dLeader, "S%d Achieved Majority for T%d , converting to Leader", rf.me, rf.currentTerm)
			}
		} else if response.Term > rf.currentTerm {
			rf.increaseTerm(response.Term)
		}
	}
	// 只发送一次，发送失败就算了
}

func (rf *Raft) begin_heartsbeats() {

	rf.heartsbeats_timeout = time.Now().UnixMilli() + get_rand_time(100, 0)
	Debug(dTimer, "S%d Begin heartsbeats", rf.me)

	lastlog := len(rf.log) - 1
	validTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := rf.genAppendEntriesArgs(peer)
		go rf.one_AppendEntries(peer, args, lastlog, validTerm)
	}

}

func (rf *Raft) one_AppendEntries(peer int, args *AppendEntriesArgs, lastlog int, validTerm int) {
	reply := AppendEntriesReply{}
	Debug(dLog, "S%d -> S%d Sending PLI:%d PLT:%d N:%d LC:%d", rf.me, peer, args.PreLogIndex, args.PreLogTerm, len(args.Entries), args.LeaderCommit)
	if rf.sendAppendEntries(peer, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//if validTerm!=rf.currentTerm || rf.currentState!=Leader {
		//	// server已经不是leader，直接跳出
		//	// 或者并非validTerm，是历史RPC的reply可以直接退出。
		//	return
		//}
		defer rf.persist()
		if reply.Term > rf.currentTerm {
			rf.increaseTerm(reply.Term)
		} else if rf.currentTerm == validTerm && rf.currentTerm == reply.Term && reply.Success {
			// 同步成功
			// 写成这样是为了考虑历史RPC的影响，导致matchIndex出现倒退的问题。
			if rf.nextIndex[peer] < lastlog+1 {
				rf.nextIndex[peer] = lastlog + 1
			}
			if rf.matcnIndex[peer] < lastlog {
				rf.matcnIndex[peer] = lastlog
			}
			Debug(dLog, "S%d <- S%d OK Append NI[%d]=%d, MI[%d]=%d", rf.me, peer, peer, rf.nextIndex[peer], peer, rf.matcnIndex[peer])

			// 检查commitIndex能否更新

			// 这里要判断是否是当前Term的log，否则不予commit
			if (rf.log[lastlog].Term == rf.currentTerm) && (lastlog > rf.commitIndex) && rf.canIncreaseCommitIndex(lastlog) {
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

// applier的后台协程，只有一个
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			//fmt.Println("Waiting")
			rf.applyCond.Wait()
		}
		//fmt.Println("Ready to commit")
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Log, commitIndex-lastApplied)
		//fmt.Println("commiting ",commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])

		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.LogIndex - 1,
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
		if rf.matcnIndex[peer] >= index {
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
	rf.matcnIndex = make([]int, len(rf.peers))

	rf.currentState = Follower
	rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300, 150)

	// 每个server填充一个空的log，用以记录snapshotIndex
	empty_log := Log{
		Term:     0,
		Command:  -1,
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
