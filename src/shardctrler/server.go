package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs           []Config // indexed by config num
	CompletedPool     map[int64]int64
	CompletedCondPool map[int64]*sync.Cond
	CommitedId        int
}
type ReqType string

const (
	JOIN  ReqType = "JOIN"
	MOVE  ReqType = "MOVE"
	LEAVE ReqType = "LEAVE"
	QUERY ReqType = "QUERY"
)

type Op struct {
	// Your data here.
	ClientId  int64
	ReqNo     int64
	ReqType   ReqType
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs
}

func removeDuplicate(strs []string) []string {
	mp := make(map[string]bool)
	for _, str := range strs {
		mp[str] = true
	}
	result := []string{}
	for str, _ := range mp {
		result = append(result, str)
	}
	return result
}

func initConfifg(cfg *Config, servers map[int][]string) {
	cfg.Groups = servers
	gNum := len(cfg.Groups)
	// unit := NShards / gNum
	gids := []int{}
	for gid, _ := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		if gids[i] != gids[j] {
			return gids[i] < gids[j]
		}
		return i < j
	})
	for i := 0; i < NShards; i++ {
		cfg.Shards[i] = gids[i%gNum]
	}
}

func rebalanceConfig(cfg *Config) {
	if len(cfg.Groups) == 0 {
		return
	}
	type GidPair struct {
		gid   int
		count int
	}
	gid2shards := make(map[int][]int)
	for shardId, gid := range cfg.Shards {
		gid2shards[gid] = append(gid2shards[gid], shardId)
	}
	gids := []GidPair{}
	// origin
	for gid, shardIds := range gid2shards {
		gids = append(gids, GidPair{gid: gid, count: len(shardIds)})
	}
	// newcome
	for gid, _ := range cfg.Groups {
		if _, exist := gid2shards[gid]; !exist {
			gids = append(gids, GidPair{gid: gid, count: 0})
		}
	}
	sort.Slice(gids, func(a, b int) bool {
		if gids[a].count != gids[b].count {
			return gids[a].count < gids[b].count
		}
		return gids[a].gid < gids[b].gid
	})
	avg := max(1, NShards/len(cfg.Groups))
	i := 0
	j := len(gids) - 1
	for i < j && len(gid2shards[gids[i].gid]) < avg && len(gid2shards[gids[j].gid]) > avg {
		need := avg - len(gid2shards[gids[i].gid])
		extra := len(gid2shards[gids[j].gid]) - avg
		part := min(need, extra)
		gid2shards[gids[i].gid] = append(gid2shards[gids[i].gid], gid2shards[gids[j].gid][0:part]...)
		gid2shards[gids[j].gid] = gid2shards[gids[j].gid][part:]
		if extra == need {
			i++
			j--
		} else if extra > need {
			i++
		} else {
			j--
		}
	}
	for gid, shardIds := range gid2shards {
		for _, shardId := range shardIds {
			cfg.Shards[shardId] = gid
		}
	}
	// debug(dInfo, "GIDs %v, Groups len %v, %v, Shards %v, gid2shards %v", gids, len(cfg.Groups), cfg.Groups, cfg.Shards, gid2shards)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (sc *ShardCtrler) doJoin(args *JoinArgs) {
	ncfg := deepCopy(&sc.configs[len(sc.configs)-1])
	ncfg.Num = len(sc.configs)
	if ncfg.Shards[0] == 0 {
		initConfifg(&ncfg, args.Servers)
	} else {
		for gid, serverNames := range args.Servers {
			ncfg.Groups[gid] = append(ncfg.Groups[gid], serverNames...)
		}
		rebalanceConfig(&ncfg)
	}
	sc.configs = append(sc.configs, ncfg)
}

func (sc *ShardCtrler) doLeave(args *LeaveArgs) {
	// init a new cfg
	ncfg := deepCopy(&sc.configs[len(sc.configs)-1])
	ncfg.Num = len(sc.configs)
	// delete from ncfg.Groups
	for _, gid := range args.GIDs {
		delete(ncfg.Groups, gid)
	}
	deletedGid := make(map[int]bool)
	for _, gid := range args.GIDs {
		deletedGid[gid] = true
	}
	// add deleted gid's shard to a temp gid
	tGid := 0
	for gid, _ := range ncfg.Groups {
		if tGid == 0 {
			tGid = gid
		} else {
			tGid = min(tGid, gid)
		}
	}
	for shardId, gid := range ncfg.Shards {
		if _, exist := deletedGid[gid]; exist {
			ncfg.Shards[shardId] = tGid
		}
	}
	// rebalance
	rebalanceConfig(&ncfg)
	sc.configs = append(sc.configs, ncfg)
}

func (sc *ShardCtrler) doMove(args *MoveArgs) {
	ncfg := deepCopy(&sc.configs[len(sc.configs)-1])
	ncfg.Num = len(sc.configs)
	ncfg.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, ncfg)
}

func (sc *ShardCtrler) doQuery(args *QueryArgs) {
	//actually do nothing here, in Query handler do real
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.ReqNo <= sc.CompletedPool[args.ClientId] {
		return
	}

	op := Op{
		ClientId: args.ClientId,
		ReqNo:    args.ReqNo,
		ReqType:  JOIN,
		JoinArgs: *args,
	}
	_, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.CompletedCondPool[args.ClientId] = sync.NewCond(&sc.mu)
	for {
		sc.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := sc.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.WrongLeader = true
			return
		}
		if completedReq, exist := sc.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.ReqNo <= sc.CompletedPool[args.ClientId] {
		return
	}
	op := Op{
		ClientId:  args.ClientId,
		ReqNo:     args.ReqNo,
		ReqType:   LEAVE,
		LeaveArgs: *args,
	}
	_, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.CompletedCondPool[args.ClientId] = sync.NewCond(&sc.mu)
	for {
		sc.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := sc.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.WrongLeader = true
			return
		}
		if completedReq, exist := sc.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.ReqNo <= sc.CompletedPool[args.ClientId] {
		return
	}
	op := Op{
		ClientId: args.ClientId,
		ReqNo:    args.ReqNo,
		ReqType:  MOVE,
		MoveArgs: *args,
	}
	_, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.CompletedCondPool[args.ClientId] = sync.NewCond(&sc.mu)
	for {
		sc.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := sc.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.WrongLeader = true
			return
		}
		if completedReq, exist := sc.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.ReqNo <= sc.CompletedPool[args.ClientId] {
		return
	}
	op := Op{
		ClientId:  args.ClientId,
		ReqNo:     args.ReqNo,
		ReqType:   QUERY,
		QueryArgs: *args,
	}
	_, startTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	sc.CompletedCondPool[args.ClientId] = sync.NewCond(&sc.mu)
	for {
		sc.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := sc.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.WrongLeader = true
			return
		}
		if completedReq, exist := sc.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			if args.Num == -1 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			break
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.CommitedId = 0
	sc.CompletedPool = make(map[int64]int64)
	sc.CompletedCondPool = make(map[int64]*sync.Cond)

	go sc.applyLog()
	go sc.BroadcastAllPeoridly()
	return sc
}

func (sc *ShardCtrler) applyLog() {
	for applyMsg := range sc.applyCh {
		sc.mu.Lock()
		if applyMsg.CommandValid {
			msg := applyMsg.Command.(Op)
			if msg.ReqNo > sc.CompletedPool[msg.ClientId] {
				switch msg.ReqType {
				case JOIN:
					sc.doJoin(&msg.JoinArgs)
				case LEAVE:
					sc.doLeave(&msg.LeaveArgs)
				case MOVE:
					sc.doMove(&msg.MoveArgs)
				case QUERY:
					sc.doQuery(&msg.QueryArgs)
				}
				sc.CompletedPool[msg.ClientId] = msg.ReqNo
				// debug(dInfo, "SC%v apply log type: %v, index: %v, config: %v", sc.me, msg.ReqType, applyMsg.CommandIndex, sc.configs[len(sc.configs)-1])
			}
			if _, exist := sc.CompletedCondPool[msg.ClientId]; exist {
				sc.CompletedCondPool[msg.ClientId].Broadcast()
			}
			sc.CommitedId = max(sc.CommitedId, applyMsg.CommandIndex)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) BroadcastAllPeoridly() {
	for {
		sc.mu.Lock()
		for _, v := range sc.CompletedCondPool {
			v.Broadcast()
		}
		sc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
