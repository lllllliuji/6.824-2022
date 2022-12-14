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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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

type RaftLogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type Role int32

const (
	Leader    Role = 0
	Follower  Role = 1
	Candidate Role = 2
)

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

	// persistent state on all servers
	applyChan chan ApplyMsg

	CurrentTerm int
	VotedFor    int
	Logs        []RaftLogEntry

	// volatile states on all servers
	CommitIndex int
	LastApplied int

	// volatile states on leader, reinitialized after election
	NextIndex  []int
	MatchIndex []int

	State Role
	// volatile states on follower
	LastReceive time.Time

	ElectionTimeout time.Duration

	Half              int
	TotalNum          int
	BaseIndex         int
	LeaderTerm        int
	LastNewEntryIndex int
	Cond              *sync.Cond
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	Conflict      bool
	ConflictTerm  int
	ConflictIndex int
	// AppendNewEntry bool
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func randElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(500)+300) * time.Millisecond
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// debug(dInfo, "S%v received appendentries from S%v", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = false
	reply.Conflict = false
	// 1. reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		debug(dError, "S%v Term %v receive from Leader S%v Term %v, refused", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		return
	}
	stateChange := false
	// turns to follower, any AppendEntry counts as a heartbeat, reset election timer
	rf.State = Follower
	rf.LastReceive = time.Now()
	rf.ElectionTimeout = randElectionTimeout()
	if args.Term != rf.LeaderTerm {
		rf.LeaderTerm = args.Term
		rf.LastNewEntryIndex = rf.CommitIndex
		rf.VotedFor = -1
		stateChange = true
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.LeaderTerm = args.Term
		rf.LastNewEntryIndex = rf.CommitIndex
		stateChange = true
	}
	if len(args.Entries) != 0 {
		debug(dInfo, "S%v received entries [%v, %v] from S%v, [%v, %v]",
			rf.me, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
	}
	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {

	} else if endIndex := rf.BaseIndex + len(rf.Logs) - 1; args.PrevLogIndex > endIndex {
		reply.Conflict = true
		reply.ConflictIndex = rf.BaseIndex + len(rf.Logs) - 1
		return
	} else if prevTerm := rf.Logs[args.PrevLogIndex-rf.BaseIndex].Term; prevTerm != args.PrevLogTerm {
		reply.Conflict = true
		reply.ConflictTerm = prevTerm
		reply.ConflictIndex = rf.Logs[args.PrevLogIndex-rf.BaseIndex].Index
		for i := args.PrevLogIndex - rf.BaseIndex; i >= 0; i-- {
			if rf.Logs[i].Term != prevTerm {
				reply.ConflictIndex = rf.Logs[i+1].Index
				break
			}
		}
		return
	}
	reply.Success = true
	if len(args.Entries) != 0 {
		// 3. delete the conflict entry and all that follow it
		i := 0
		index := args.PrevLogIndex - rf.BaseIndex + 1
		for index < len(rf.Logs) && i < len(args.Entries) {
			if rf.Logs[index] != args.Entries[i] {
				break
			}
			index++
			i++
		}
		// 4. append any entries not already in log
		if i < len(args.Entries) {
			rf.Logs = rf.Logs[0:index]
			rf.Logs = append(rf.Logs, args.Entries[i:]...)
			stateChange = true
		}
		// if args.PrevLogIndex+len(args.Entries) > rf.LastNewEntryIndex {
		// 	rf.LastNewEntryIndex = args.PrevLogIndex + len(args.Entries)
		// 	reply.AppendNewEntry = true
		// }
		rf.LastNewEntryIndex = max(rf.LastNewEntryIndex, args.PrevLogIndex+len(args.Entries))
		debug(dLog, "S%v received log [%v, %v] from S%v, commitId %v leaderCommitId %v",
			rf.me, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, args.LeaderId, rf.CommitIndex, args.LeaderCommit)
	}
	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.commit(min(args.LeaderCommit, rf.LastNewEntryIndex))
	}
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	if rf.CommitIndex > rf.LastApplied {
		rf.LastApplied++
	}
	if stateChange {
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// debug(dLog, "S%v sendAppendEntries to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if peer's term greater than rf.currentTerm, turn to follower
	if reply.Term > rf.CurrentTerm {
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.LeaderTerm = reply.Term
		rf.LastNewEntryIndex = rf.CommitIndex
		rf.persist()
		return false
	} else if reply.Term < rf.CurrentTerm {
		return false
	}
	// rf is still the leader, update
	if rf.CurrentTerm == args.Term {
		if reply.Success {
			rf.MatchIndex[server] = max(rf.MatchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.NextIndex[server] = max(rf.NextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
			// if len(args.Entries) != 0 {
			// 	debug(dLog, "Leader S%v successfully replicated log [%v, %v] to S%v",
			// 		rf.me, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index, server)
			// }
		} else if reply.Conflict && rf.NextIndex[server] == args.PrevLogIndex+1 {
			debug(dLog, "S%v conflicted with Leader S%v conflictIndex %v", server, rf.me, reply.ConflictIndex)
			if reply.ConflictTerm != 0 {
				found := false
				index := 0
				for index < len(rf.Logs) {
					if rf.Logs[index].Term == reply.ConflictTerm {
						found = true
						break
					}
					index++
				}
				if found {
					for index < len(rf.Logs) && rf.Logs[index].Term == reply.ConflictTerm {
						index++
					}
					// term 单调性保证 index不会超过rf.logs的下标
					rf.NextIndex[server] = rf.Logs[index].Index
				} else {
					rf.NextIndex[server] = reply.ConflictIndex
				}
			} else {
				rf.NextIndex[server] = max(reply.ConflictIndex, 1)
			}
		}
	}
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
	return term, isleader
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
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Logs)
	e.Encode(rf.VotedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// debug(dInfo, "S%v persist after killed, raftsate length: %v", rf.me, len(rf.persister.raftstate))
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VotedFor int
	var Logs []RaftLogEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&Logs) != nil || d.Decode(&VotedFor) != nil {
		debug(dError, "Error when decode")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Logs = Logs
	}
	debug(dInfo, "S%v readPersist", rf.me)
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
	CandidateId  int
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
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// debug(dLog, "S%v receive request vote from S%v", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.State = Follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.LeaderTerm = args.Term
		rf.LastNewEntryIndex = rf.CommitIndex
		rf.persist()
	}
	// debug(dVote, "S%v term %v received RequestVote from S%v term %v", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		lastLogTerm := 0
		lastLogIndex := 0
		if len(rf.Logs) != 0 {
			lastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			lastLogIndex = rf.Logs[len(rf.Logs)-1].Index
		}
		if args.LastLogTerm < lastLogTerm {
			return
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
			return
		}
		// vote for this candidate and reset election timer
		debug(dVote, "S%v term %v voted for S%v in term %v", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
		rf.VotedFor = args.CandidateId
		rf.LastReceive = time.Now()
		rf.ElectionTimeout = randElectionTimeout()
		reply.VoteGranted = true
		rf.persist()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		return index, term, false
	}
	// construct rquest's command to log entry
	raftLogEntry := RaftLogEntry{
		Command: command,
		Term:    rf.CurrentTerm,
	}
	if len(rf.Logs) == 0 {
		raftLogEntry.Index = rf.BaseIndex
	} else {
		raftLogEntry.Index = rf.Logs[len(rf.Logs)-1].Index + 1
	}
	// append and update
	rf.Logs = append(rf.Logs, raftLogEntry)
	rf.NextIndex[rf.me] = raftLogEntry.Index + 1
	rf.MatchIndex[rf.me] = raftLogEntry.Index
	debug(dInfo, "Leader S%v replicated log %v %v", rf.me, raftLogEntry.Index, raftLogEntry.Command)
	rf.persist()
	// go rf.startAgreement(raftLogEntry, rf.nextIndex[rf.me])
	return raftLogEntry.Index, raftLogEntry.Term, true
}

func (rf *Raft) commit(endIndex int) {
	// debug(dLog, "S%v applier", rf.me)
	// applyMsgs := []ApplyMsg{}
	for i := rf.CommitIndex + 1; i <= endIndex; i++ {
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.Command = rf.Logs[i-rf.BaseIndex].Command
		applyMsg.CommandIndex = rf.Logs[i-rf.BaseIndex].Index
		// applyMsgs = append(applyMsgs, applyMsg)
		rf.applyChan <- applyMsg
		debug(dInfo, "S%v apply Log %v %v", rf.me, applyMsg.CommandIndex, applyMsg.Command)
	}
	// go func(applyMsgs []ApplyMsg) {
	// 	for _, msg := range applyMsgs {
	// 		rf.applyChan <- msg
	// 		debug(dInfo, "S%v apply Log %v", rf.me, msg.CommandIndex)
	// 	}
	// }(applyMsgs)
	rf.CommitIndex = max(rf.CommitIndex, endIndex)
}

func (rf *Raft) becomeLeader() {
	// init matchIndex, nextIndex
	for i, _ := range rf.NextIndex {
		if i != rf.me {
			rf.MatchIndex[i] = 0
		} else {
			if len(rf.Logs) == 0 {
				rf.MatchIndex[i] = 0
			} else {
				rf.MatchIndex[i] = rf.Logs[len(rf.Logs)-1].Index
			}
		}
		if len(rf.Logs) == 0 {
			rf.NextIndex[i] = 1
		} else {
			rf.NextIndex[i] = rf.Logs[len(rf.Logs)-1].Index + 1
		}
	}
	rf.sendHeartBeat()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.replicateLog(i, rf.CurrentTerm)
		}
	}
	go rf.updateCommitIndex(rf.CurrentTerm)
}

func (rf *Raft) replicateLog(server, leaderTerm int) {
	cond := sync.NewCond(&rf.mu)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CurrentTerm != leaderTerm {
			debug(dLog, "Leader changed rf.currentTerm %v leaderTerm %v, S%v stop replicate log to follower S%v",
				rf.CurrentTerm, leaderTerm, rf.me, server)
			rf.mu.Unlock()
			return
		}
		if rf.NextIndex[server] != rf.NextIndex[rf.me] {
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      rf.Logs[rf.NextIndex[server]-rf.BaseIndex: /*min(rf.NextIndex[server]-rf.BaseIndex+100, len(rf.Logs))*/],
				LeaderCommit: rf.CommitIndex,
			}
			if rf.NextIndex[server] != rf.BaseIndex {
				args.PrevLogIndex = rf.Logs[rf.NextIndex[server]-1-rf.BaseIndex].Index
				args.PrevLogTerm = rf.Logs[rf.NextIndex[server]-1-rf.BaseIndex].Term
			}
			reply := AppendEntriesReply{}
			go func(srv int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				rf.sendAppendEntries(srv, args, reply)
				rf.mu.Lock()
				cond.Broadcast()
				rf.mu.Unlock()
			}(server, &args, &reply)
			cond.Wait()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex(leaderTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CurrentTerm != leaderTerm {
			debug(dLog, "Leader changed rf.currentTerm %v leaderTerm %v, Leader S%v stop update commitIndex",
				rf.CurrentTerm, leaderTerm, rf.me)
			rf.mu.Unlock()
			return
		}
		for i := len(rf.Logs) - 1; i >= 0; i-- {
			// leader doesn't commit log from its previous leader
			if rf.Logs[i].Term != leaderTerm {
				break
			}
			count := 0
			for j := 0; j < len(rf.peers); j++ {
				if rf.MatchIndex[j] >= rf.Logs[i].Index {
					count++
				}
				if count > rf.Half {
					break
				}
			}
			if count > rf.Half {
				rf.commit(rf.Logs[i].Index)
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
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

func (rf *Raft) sendHeartBeat() {
	// debug(dInfo, "S%v send heartbearts", rf.me)
	for i := 0; i < rf.TotalNum; i++ {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []RaftLogEntry{},
				LeaderCommit: rf.CommitIndex,
			}
			reply := AppendEntriesReply{}
			if len(rf.Logs) != 0 && rf.NextIndex[i] != rf.BaseIndex {
				args.PrevLogIndex = rf.Logs[rf.NextIndex[i]-1-rf.BaseIndex].Index
				args.PrevLogTerm = rf.Logs[rf.NextIndex[i]-1-rf.BaseIndex].Term
			}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

func (rf *Raft) issueLeaderElection(templateArgs RequestVoteArgs, templateReply RequestVoteReply) {
	debug(dVote, "S%v in term %v issued a election", templateArgs.CandidateId, templateArgs.Term)
	votes := 1
	done := false
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
				ok := rf.sendRequestVote(server, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					votes++
					if done || votes <= rf.Half {
						return
					}
					if rf.CurrentTerm != args.Term {
						return
					}
					done = true
					rf.State = Leader
					debug(dVote, "S%v got enough votes(%v), became leader of term %v", rf.me, votes, rf.CurrentTerm)
					rf.becomeLeader()
				} else if reply.Term > rf.CurrentTerm {
					rf.State = Follower
					rf.CurrentTerm = reply.Term
					rf.VotedFor = -1
					rf.LeaderTerm = reply.Term
					rf.LastNewEntryIndex = rf.CommitIndex
					rf.persist()
				}
			}(i, templateArgs, templateReply)
		}
	}
}

func (rf *Raft) prepareElection() (RequestVoteArgs, RequestVoteReply) {
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.State = Candidate
	rf.LastReceive = time.Now()
	rf.ElectionTimeout = randElectionTimeout()
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.CandidateId = rf.me
	args.Term = rf.CurrentTerm
	if len(rf.Logs) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = rf.Logs[len(rf.Logs)-1].Index
		args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}
	rf.persist()
	return args, reply
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// debug(dLog, "S%v term %v Lock", rf.me, rf.currentTerm)
		switch rf.State {
		case Follower:
			if time.Since(rf.LastReceive) > rf.ElectionTimeout {
				args, reply := rf.prepareElection()
				go rf.issueLeaderElection(args, reply)
			}
		case Leader:
			// send heart beat
			rf.sendHeartBeat()
		case Candidate:
			if time.Since(rf.LastReceive) > rf.ElectionTimeout {
				args, reply := rf.prepareElection()
				go rf.issueLeaderElection(args, reply)
			}
		}
		rf.mu.Unlock()
		// debug(dLog, "S%v term %v UnLock", rf.me, rf.currentTerm)
		time.Sleep(100 * time.Millisecond)
	}
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.State = Follower
	rf.LastReceive = time.Now()
	rf.ElectionTimeout = randElectionTimeout()
	rf.applyChan = applyCh
	rf.Half = len(rf.peers) / 2
	rf.TotalNum = len(rf.peers)
	rf.BaseIndex = 1
	rf.LeaderTerm = -1
	rf.LastNewEntryIndex = 0
	rf.Cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.persist()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
