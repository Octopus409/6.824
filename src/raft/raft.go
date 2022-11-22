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
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	// 其他一些包
	"time"
	//"fmt"
	"math/rand"
)


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

func min(v1 int,v2 int)int{
	if v1 < v2 {
		return v1
	}
	return v2
}

func max(v1 int,v2 int)int{
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
	Term int
	Command interface{}
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

	applyCh chan ApplyMsg
	applyCond *sync.Cond   // applier的条件变量

	// 根据论文，给raft设置state
	currentTerm int
	votedFor int
	log []Log
	commitIndex int
	lastApplied int

	nextIndex []int
	matcnIndex []int

	currentState int  // 当前状态
	election_timeout int64  // 选举超时时间
	heartsbeats_timeout int64  // 心跳超时时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState==Leader {
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
}


//
// restore previously persisted state.
//
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
	Term int
	CandidateId int  // 候选人id
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool   // 是否投你
}


// AppendEntries
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term int
	Success bool  // true代表同步成功
}

func (rf *Raft)isLogUpToDate(LastLogTerm int,LastLogIndex int) bool {
	myLastLogIndex := len(rf.log)-1
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	if LastLogTerm > myLastLogTerm {
		return true
	} else if LastLogTerm==myLastLogTerm && LastLogIndex>=myLastLogIndex{
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
	DPrintf(" S%d RequestVote from S%d",rf.me,args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.currentState==Candidate || rf.currentState==Leader {
			// 同Term，Candidate和Leader不投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		if rf.votedFor == -1 || rf.votedFor==args.CandidateId {
			// 可以投票
			// ===================日志判断
			reply.Term = rf.currentTerm
			if rf.isLogUpToDate(args.LastLogTerm,args.LastLogIndex) {  // 只有grant成功，才更新选举时间
				rf.votedFor = args.CandidateId
				rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150) 
				reply.VoteGranted = true;
			} else {
				reply.VoteGranted = false;
			}
			
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		
	} else if args.Term > rf.currentTerm {
		// 进入新Term、设置voteFor、重置选举时间
		// 转为Follower
		// 判断日志，返回True或false

		DPrintf(" S%d Term update (from %d to %d)",rf.me,rf.currentTerm,args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.currentState==Leader || rf.currentState==Candidate{
			switch rf.currentState{
			case Leader:
				DPrintf(" S%d State change Leader -> Follower",rf.me)
			case Candidate:
				DPrintf(" S%d State change Candidate -> Follower",rf.me)
			}
			rf.currentState = Follower
		}
		rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)

		// ======================日志对比
		if rf.isLogUpToDate(args.LastLogTerm,args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		
		
	}
}

func (rf *Raft) isLogMatch(PreLogTerm int,PreLogIndex int) bool {
	lastlog := len(rf.log)-1
	if lastlog >= PreLogIndex && rf.log[PreLogIndex].Term == PreLogTerm{
		return true
	}
	return false
}

func (rf *Raft) updateLog(PreLogIndex int,log []Log){
	// 遵循不截断原则，除非出现不同，否则不截断后面的。
	tmplog := rf.log[0:PreLogIndex+1]

	/*
	var isDiff bool
	for i,l := range log {
		if l.Term==rf.log[PreLogIndex+1+i]
	}
	*/
	for _,l := range log {
		tmplog = append(tmplog,l)
	}

	/*
	if PreLogIndex + len(log) < len(rf.log) -1 {
		// 补齐后面的

	}
	*/

	// 忽略历史报文，直接拼接试试
	rf.log = tmplog
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply* AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(" S%d AppendEntries from S%d",rf.me,args.LeaderId)
	if args.Term < rf.currentTerm {
		// 礼貌回复一下就行
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if args.Term == rf.currentTerm {
		// 保持这个Term、重置选举时间
		// Follower要保持Follower，Candidate要降级为Follower，Leader应该是不可能收到这个的
		// ===================日志对比
		if rf.isLogMatch(args.PreLogTerm,args.PreLogIndex) {
			// 匹配的话，要进行日志更新。
			rf.updateLog(args.PreLogIndex,args.Entries)
			if args.LeaderCommit > rf.commitIndex {
				
				rf.commitIndex = min(args.LeaderCommit,len(rf.log)-1)
				//fmt.Println("Im ",rf.me," LeaderCommit is ",args.LeaderCommit," .New CommitIndex is ",rf.commitIndex)
				rf.applyCond.Signal()
			} 
			reply.Term = rf.currentTerm
			reply.Success = true
		} else {
			// 不匹配，返回false，等一下轮更新
			reply.Term = rf.currentTerm
			reply.Success = false
		}

		if rf.currentState== Candidate{
			rf.currentState = Follower
			DPrintf(" S%d State change Canditate -> Follower",rf.me)
		}
		rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)
	} else if args.Term > rf.currentTerm {
		// 进入新Term、设置voteFor、重置选举时间
		// Follower保持Follower，其他降级为Follower
		// 判断日志，依据返回True和False
		// ===================日志对比
		if rf.isLogMatch(args.PreLogTerm,args.PreLogIndex) {
			// 匹配的话，要进行日志更新。
			rf.updateLog(args.PreLogIndex,args.Entries)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit,len(rf.log)-1)
			} 
			reply.Term = rf.currentTerm
			reply.Success = true
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}

		DPrintf(" S%d Term update (from %d to %d)",rf.me,rf.currentTerm,args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.currentState==Leader || rf.currentState==Candidate{
			switch rf.currentState{
			case Leader:
				DPrintf(" S%d State change Leader -> Follower",rf.me)
			case Candidate:
				DPrintf(" S%d State change Candidate -> Follower",rf.me)
			}
			rf.currentState = Follower
		}
		rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)
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
	ok := rf.peers[server].Call("Raft.AppendEntries",args,reply)
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
	if rf.currentState!=Leader {
		return -1,-1,false
	}
	index = len(rf.log)
	logEntry := Log{
		Term: rf.currentTerm,
		Command: command,
		LogIndex: index+1,
	}
	rf.log = append(rf.log,logEntry)

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
	for rf.killed() == false {

		// 每10毫秒检测一次选举超时
		time.Sleep(10*time.Millisecond)
		//fmt.Println("Now time: ",time.Now().Format("15:04:05.000"))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		
		if rf.currentState!=Leader && time.Now().UnixMilli()>= rf.election_timeout {
			// 发动选举
			// 把操作封装在一个函数

			rf.begin_eletion()
			
		} else if rf.currentState==Leader && time.Now().UnixMilli()>=rf.heartsbeats_timeout{

			// 发起心跳
			rf.begin_heartsbeats()
		} 

		rf.mu.Unlock()
	}
}

func (rf *Raft) genRequestVoteArgs()*RequestVoteArgs{
	// 不加锁，默认调用这个的时候，已经加锁
	res := RequestVoteArgs{}
	res.Term = rf.currentTerm
	res.CandidateId = rf.me
	res.LastLogIndex = len(rf.log)-1
	res.LastLogTerm = rf.log[len(rf.log)-1].Term
	return &res
}

func (rf *Raft) genAppendEntriesArgs(peer int)*AppendEntriesArgs{
	// 不加锁，默认调用这个的时候，已经加锁
	res := AppendEntriesArgs{}
	res.Term = rf.currentTerm
	res.LeaderId = rf.me
	// Log[]、PreLogIndex、PreLogTerm关联于不同发送对象
	if (len(rf.log)-1)>=rf.nextIndex[peer] {
		// 有log要发送
		for i:=rf.nextIndex[peer]; i<len(rf.log); i++{
			res.Entries = append(res.Entries,rf.log[i])
		}
	}
	res.PreLogIndex = rf.nextIndex[peer]-1
	res.PreLogTerm = rf.log[rf.nextIndex[peer]-1].Term

	res.LeaderCommit = rf.commitIndex

	return &res
}

func (rf *Raft) begin_eletion(){

	rf.currentState = Candidate
	rf.currentTerm++   // 增加term
	rf.election_timeout += get_rand_time(300,150) // 选举时间增加300多ms

	DPrintf(" S%d begin_election",rf.me)
	DPrintf(" S%d Term update (from %d to %d)",rf.me,rf.currentTerm-1,rf.currentTerm)

	// 使用协程发动选举
	// 需要对candidate当前状态进行一个快照，也就是requestVote的args要保存
	args := rf.genRequestVoteArgs()

	grantedVote := 1
	rf.votedFor = rf.me
	for peer := range rf.peers {
		if peer==rf.me {
			continue
		}
		go rf.one_RequestVote(peer,args,&grantedVote)
	}
}

func (rf *Raft) one_RequestVote(peer int,args *RequestVoteArgs,grantedVote *int){
	// 创建发送args和reply的数组
	response := RequestVoteReply{}
	if rf.sendRequestVote(peer,args,&response) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentState!=Candidate {
			// 成为Leader或者竞选失败后，后续发送已没有意义。
			return
		}
		if response.VoteGranted {
			// 对面投票了，说明他的Term小于等于我的
			*grantedVote++
			if *grantedVote > len(rf.peers)/2 {
				rf.currentState = Leader
				// 成为leader，不需要管election_timeout，这个等下一次变为follower时再更新
				// 成为leader，要初始化nextIndex[]和matchIndex[]
				lastlog := len(rf.log)-1
				for peer := range rf.peers {
					rf.nextIndex[peer] = lastlog + 1
					rf.matcnIndex[peer] = 0
				}

				// 开启发送心跳报文
				rf.heartsbeats_timeout = time.Now().UnixMilli()

				DPrintf(" S%d State change Candidate -> Leader",rf.me)
			}
		} else if response.Term > rf.currentTerm {
			DPrintf(" S%d Term update (from %d to %d)",rf.me,rf.currentTerm,response.Term)
			rf.currentTerm = response.Term
			rf.votedFor = -1

			// 收到更新的Term，降为Follower，更新election_time
			if rf.currentState==Candidate{
				rf.currentState = Follower
				DPrintf(" S%d State change Candidate -> Follower",rf.me)
			}
			rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)
		}
	}
	// 只发送一次，发送失败就算了
	return
}

func (rf *Raft) begin_heartsbeats(){

	rf.heartsbeats_timeout += get_rand_time(100,0)
	DPrintf(" S%d begin_heartsbeats",rf.me)

	lastlog := len(rf.log)-1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := rf.genAppendEntriesArgs(peer)
		go rf.one_AppendEntries(peer,args,lastlog)
	}

}

func (rf *Raft) one_AppendEntries(peer int,args *AppendEntriesArgs,lastlog int){
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer,args,&reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Success == true {
			// 同步成功
			rf.nextIndex[peer] = lastlog+1
			rf.matcnIndex[peer] = lastlog
			// 检查commitIndex能否更新
			if lastlog > rf.commitIndex && rf.canIncreaseCommitIndex(lastlog){
				//fmt.Println("increasing the commitIndex")
				rf.commitIndex = lastlog
				rf.applyCond.Signal()
			}
		} else {
			if reply.Term == rf.currentTerm {
				// 日志匹配失败，降低nextIndex
				rf.nextIndex[peer]--
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.currentState = Follower
				rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)
			}
		}
	}
	// 发送失败同样不处理，等下一次心跳
	return
}

// applier的后台协程，只有一个
func (rf *Raft)applier(){
	for rf.killed() == false {
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
				CommandIndex: entry.LogIndex-1,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		DPrintf(" S%d applied (from %d to %d)",rf.me,lastApplied,commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft)canIncreaseCommitIndex(index int)bool{
	//fmt.Println("Checking weather can increase the commitIndex")
	sum := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.matcnIndex[peer]>=index {
			sum++
		}
	}
	if sum > len(rf.peers)/2 {
		return true
	}
	return false
}

func get_rand_time(base int,ran int)int64{
	// 20ms为一个间隔，不能靠太近
	if ran==0 {
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
	rf.log = make([]Log,0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int,len(rf.peers))
	rf.matcnIndex = make([]int,len(rf.peers))

	rf.currentState = Follower
	rf.election_timeout = time.Now().UnixMilli() + get_rand_time(300,150)
	

	// log要填充一个空的log
	empty_log := Log{
		Term: 0,
		Command: -1,
		LogIndex: 0,
	}
	rf.log = append(rf.log,empty_log)

	// 初始化调试的日志
	initLog()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
