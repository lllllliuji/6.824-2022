package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type OpType string

const (
	GET    OpType = "GET"
	PUT    OpType = "PUT"
	APPEND OpType = "APPEND"
	CONFIG OpType = "CONFIG"
)

type KVState struct {
	KVMap      map[string]string
	Config     shardctrler.Config
	KeepShards map[int]bool
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       OpType
	GetArg       GetArgs
	PutAppendArg PutAppendArgs
	ClientId     int64
	ReqNo        int64
	KvState      KVState
	Config       shardctrler.Config
}

type Result struct {
	Err   Err
	Value string
}

type ShiftShard struct {
	Shard int
	KVMap map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.KVMap             map[string]string
	mck *shardctrler.Clerk

	Config     shardctrler.Config
	Shards     map[int]bool
	ShardKVMap map[string]string

	CompletedPool     map[int64]int64
	CompletedResult   map[int64]Result
	CompletedCondPool map[int64]*sync.Cond

	CommitedId int

	ConfigReqNo   int64
	IncomeShards  map[int]map[int]ShiftShard
	OutcomeShards map[int]map[int]ShiftShard
	ConfigMu      sync.Mutex
}

func (skv *ShardKV) cleaner() {
	for {
		// delete key
		skv.mu.Lock()
		curConfigNum := skv.Config.Num
		extraKeys := []string{}
		for key := range skv.ShardKVMap {
			if !skv.Shards[key2shard(key)] {
				extraKeys = append(extraKeys, key)
			}
		}
		for _, key := range extraKeys {
			delete(skv.ShardKVMap, key)
		}
		skv.mu.Unlock()
		// delete config pool
		skv.ConfigMu.Lock()
		expiredConfigNum := make(map[int]bool)
		for configNum := range skv.IncomeShards {
			if configNum <= curConfigNum {
				expiredConfigNum[configNum] = true
			}
		}
		for configNum := range skv.OutcomeShards {
			if configNum <= curConfigNum {
				expiredConfigNum[configNum] = true
			}
		}
		for configNum := range expiredConfigNum {
			delete(skv.IncomeShards, configNum)
			delete(skv.OutcomeShards, configNum)
		}
		skv.ConfigMu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (skv *ShardKV) MigrateShards(args *ShiftShardArgs, reply *ShiftShardReply) {
	skv.ConfigMu.Lock()
	defer skv.ConfigMu.Unlock()
	if skv.IncomeShards[args.ConfigNum] == nil {
		skv.IncomeShards[args.ConfigNum] = make(map[int]ShiftShard)
	}
	skv.IncomeShards[args.ConfigNum][args.Shard] = ShiftShard{Shard: args.Shard, KVMap: args.KVMap}
	if _, isLeader := skv.rf.GetState(); isLeader {
		reply.Success = true
		reply.IsLeader = true
		debug(dInfo, "G%v S%v migrateshards configNum: %v shard: %v", skv.gid, skv.me, args.ConfigNum, args.Shard)
	}
}

func (skv *ShardKV) checkInShards(complete chan<- bool, curConfig, config shardctrler.Config, inShards map[int]bool) {
	// debug(dInfo, "GID: %v, SK%v configNum: %v, checkInshards", skv.gid, skv.me, config.Num)
	for {
		if curConfig.Shards[0] == 0 {
			break
		}
		skv.ConfigMu.Lock()
		receivedShards := make(map[int]bool)
		for _, ShiftShard := range skv.IncomeShards[config.Num] {
			receivedShards[ShiftShard.Shard] = true
		}
		completeShift := false
		if len(receivedShards) == len(inShards) {
			same := true
			for key := range inShards {
				if _, exist := receivedShards[key]; !exist {
					same = false
					break
				}
			}
			if same {
				completeShift = true
			}
		}
		skv.ConfigMu.Unlock()
		if completeShift {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	debug(dInfo, "G%v S%v configNum: %v checkInshards", skv.gid, skv.me, config.Num)
	complete <- true
}

func (skv *ShardKV) checkOutShards(complete chan<- bool, config shardctrler.Config, outShards map[int]bool) {
	// debug(dInfo, "GID: %v, SK%v configNum: %v, checkOutshards", skv.gid, skv.me, config.Num)
	skv.mu.Lock()
	shard2KV := make(map[int]map[string]string)
	for shard := range outShards {
		shard2KV[shard] = make(map[string]string)
	}
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if !outShards[shard] {
			continue
		}
		shard2KV[shard][key] = value
		// stop server outShards
		skv.Shards[shard] = false
	}
	skv.mu.Unlock()
	for shard, kvMap := range shard2KV {
		args := ShiftShardArgs{
			ConfigNum: config.Num,
			Shard:     shard,
			KVMap:     kvMap,
		}
		reply := ShiftShardReply{}
		gid := config.Shards[shard]
		for {
			retry := true
			if servers, exist := config.Groups[gid]; exist {
				for si := 0; si < len(servers); si++ {
					srv := skv.make_end(servers[si])
					ok := srv.Call("ShardKV.MigrateShards", &args, &reply)
					if ok && reply.Success && reply.IsLeader {
						retry = false
						break
					}
				}
			}
			if !retry {
				break
			}
		}
	}
	// debug(dInfo, "GID: %v, SK%v configNum: %v, checkOutshards completed", skv.gid, skv.me, config.Num)
	complete <- true
}

func (skv *ShardKV) makeNewMap(inShards map[int]bool, configNum int) map[string]string {
	retMap := make(map[string]string)
	skv.ConfigMu.Lock()
	for shard, shiftShard := range skv.IncomeShards[configNum] {
		if inShards[shard] {
			for key, value := range shiftShard.KVMap {
				retMap[key] = value
			}
		}
	}
	skv.ConfigMu.Unlock()
	return retMap
}

func (skv *ShardKV) compareConfig(config shardctrler.Config) (map[int]bool, map[int]bool, map[int]bool) {
	outShards := make(map[int]bool)
	inShards := make(map[int]bool)
	keepShards := make(map[int]bool)
	for shard, gid := range config.Shards {
		if gid != skv.gid && skv.Shards[shard] {
			outShards[shard] = true
		}
		if gid == skv.gid && !skv.Shards[shard] {
			inShards[shard] = true
		}
		if gid == skv.gid && skv.Shards[shard] {
			keepShards[shard] = true
		}
	}
	return inShards, outShards, keepShards
}

func (skv *ShardKV) makeConfigAgreement(inShards, keepShards map[int]bool, config shardctrler.Config) {
	op := Op{
		OpType:   CONFIG,
		ClientId: 0,
		ReqNo:    atomic.AddInt64(&skv.ConfigReqNo, 1),
		Config:   config,
		KvState:  KVState{KVMap: skv.makeNewMap(inShards, config.Num), Config: config, KeepShards: keepShards},
	}
	for {
		skv.mu.Lock()
		retry := true
		_, startTerm, isLeader := skv.rf.Start(op)
		if !isLeader {
			break
		}
		skv.CompletedCondPool[0] = sync.NewCond(&skv.mu)
		for {
			skv.CompletedCondPool[0].Wait()
			curTerm, isLeader := skv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				break
			}
			if completedReq, exist := skv.CompletedPool[0]; exist && completedReq == op.ReqNo {
				retry = false
				break
			}
		}
		skv.mu.Unlock()
		if !retry {
			break
		}
	}
}

func (skv *ShardKV) updateConfig() {
	for {
		for {
			_, isLeader := skv.rf.GetState()
			if !isLeader {
				break
			}
			// debug(dInfo, "GID: %v, S%v config: %v, Shards: %v", skv.gid, skv.me, skv.Config, skv.Shards)
			skv.mu.Lock()
			latestConfig := skv.mck.Query(skv.Config.Num + 1)
			if latestConfig.Num <= skv.Config.Num {
				skv.mu.Unlock()
				break
			}
			inShards, outShards, keepShards := skv.compareConfig(latestConfig)
			skv.mu.Unlock()
			// shift shard in and out
			inShardComplete := make(chan bool)
			outShardComplete := make(chan bool)
			go skv.checkInShards(inShardComplete, skv.Config, latestConfig, inShards)
			go skv.checkOutShards(outShardComplete, latestConfig, outShards)
			// wait in and out complete
			<-inShardComplete
			<-outShardComplete
			// propagation in replica group
			skv.makeConfigAgreement(inShards, keepShards, latestConfig)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ReqNo <= skv.CompletedPool[args.ClientId] {
		reply.Err = OK
		reply.Value = skv.CompletedResult[args.ClientId].Value
		debug(dInfo, "G%v S%v receive duplicated request ReqNo: %v CompletedReqNo: %v from C%v",
			skv.gid, skv.me, args.ReqNo, skv.CompletedPool[args.ClientId], args.ClientId)
		return
	}
	debug(dInfo, "G%v S%v receive Get Request Get %v Shard: %v, from C%v",
		skv.gid, skv.me, args.Key, key2shard(args.Key), args.ClientId)
	op := Op{
		OpType:   GET,
		GetArg:   *args,
		ClientId: args.ClientId,
		ReqNo:    args.ReqNo,
	}
	index, startTerm, isLeader := skv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	skv.CompletedCondPool[args.ClientId] = sync.NewCond(&skv.mu)
	for {
		skv.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := skv.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if completedReq, exist := skv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
	reply.Err = skv.CompletedResult[args.ClientId].Err
	reply.Value = skv.CompletedResult[args.ClientId].Value
	debug(dInfo, "G%v S%v complete Get Request %v: %v Shard: %v index: %v",
		skv.gid, skv.me, args.Key, reply.Value, key2shard(args.Key), index)
}

func (skv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ReqNo <= skv.CompletedPool[args.ClientId] {
		reply.Err = OK
		debug(dInfo, "G%v S%v receive duplicated request ReqNo: %v CompletedReqNo: %v from C%v",
			skv.gid, skv.me, args.ReqNo, skv.CompletedPool[args.ClientId], args.ClientId)
		return
	}
	debug(dInfo, "G%v S%v receive %v Request [%v, %v] Shard: %v from C%v ReqNo: %v",
		skv.gid, skv.me, args.Op, args.Key, args.Value, args.ShardId, args.ClientId, args.ReqNo)
	op := Op{
		OpType:       PUT,
		PutAppendArg: *args,
		ClientId:     args.ClientId,
		ReqNo:        args.ReqNo,
	}
	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}
	index, startTerm, isLeader := skv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	skv.CompletedCondPool[args.ClientId] = sync.NewCond(&skv.mu)
	for {
		skv.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := skv.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if completedReq, exist := skv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
	reply.Err = skv.CompletedResult[args.ClientId].Err
	debug(dInfo, "G%v S%v complete %v Request [%v, %v] Shard %v from C%v, ReqNo: %v index: %v err: %v",
		skv.gid, skv.me, args.Op, args.Key, args.Value, args.ShardId, args.ClientId, args.ReqNo, index, reply.Err)
}

func (skv *ShardKV) BroadcastAllPeoridly() {
	for {
		skv.mu.Lock()
		for _, v := range skv.CompletedCondPool {
			v.Broadcast()
		}
		skv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (skv *ShardKV) Kill() {
	skv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Result{})
	labgob.Register(shardctrler.Config{})

	skv := new(ShardKV)
	skv.me = me
	skv.maxraftstate = maxraftstate
	skv.make_end = make_end
	skv.gid = gid
	skv.ctrlers = ctrlers

	// Your initialization code here.
	skv.Config = shardctrler.Config{}
	skv.Shards = make(map[int]bool)
	skv.ShardKVMap = make(map[string]string)

	skv.CompletedPool = make(map[int64]int64)
	skv.CompletedResult = make(map[int64]Result)
	skv.CompletedCondPool = make(map[int64]*sync.Cond)

	skv.CommitedId = 0

	skv.ConfigReqNo = 0
	skv.IncomeShards = make(map[int]map[int]ShiftShard)
	skv.OutcomeShards = make(map[int]map[int]ShiftShard)
	skv.mck = shardctrler.MakeClerk(ctrlers)

	skv.ReadSnapshot(persister.ReadSnapshot())
	// debug(dInfo, "G%v S%v len(raftstate): %v", skv.gid, skv.me, len(persister.ReadRaftState()))
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	skv.applyCh = make(chan raft.ApplyMsg)
	skv.rf = raft.Make(servers, me, persister, skv.applyCh)

	go skv.Snapshot()
	go skv.applyLog()
	go skv.updateConfig()
	go skv.cleaner()
	go skv.BroadcastAllPeoridly()
	return skv
}

func (skv *ShardKV) ReadSnapshot(data []byte) {
	// debug(dInfo, "G%v S%v readsnapshot", skv.gid, skv.me)
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Config shardctrler.Config
	var CommitedId int
	var ConfigReqNo int64
	var ShardIds map[int]bool
	var ShardKVMap map[string]string
	var CompletedPool map[int64]int64
	var CompletedResult map[int64]Result

	if d.Decode(&Config) != nil || d.Decode(&CommitedId) != nil || d.Decode(&ConfigReqNo) != nil ||
		d.Decode(&ShardIds) != nil || d.Decode(&ShardKVMap) != nil || d.Decode(&CompletedPool) != nil || d.Decode(&CompletedResult) != nil {
		debug(dError, "G%v S%v Error when decode in ReadSnapshot", skv.gid, skv.me)
		return
	} else {
		skv.Config = Config
		skv.CommitedId = CommitedId
		skv.ConfigReqNo = ConfigReqNo
		skv.Shards = ShardIds
		skv.ShardKVMap = ShardKVMap
		skv.CompletedPool = CompletedPool
		skv.CompletedResult = CompletedResult
	}
	skv.rf.Snapshot(skv.CommitedId, data)
	debug(dInfo, "G%v S%v ReadSnapshot Index: %v", skv.gid, skv.me, CommitedId)
}

func (skv *ShardKV) Snapshot() {
	for {
		skv.mu.Lock()
		if skv.maxraftstate == -1 || skv.rf.GetRaftStateSize() < skv.maxraftstate {
			skv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(skv.Config)
		e.Encode(skv.CommitedId)
		e.Encode(skv.ConfigReqNo)
		e.Encode(skv.Shards)
		e.Encode(skv.ShardKVMap)
		e.Encode(skv.CompletedPool)
		e.Encode(skv.CompletedResult)
		data := w.Bytes()
		skv.rf.Snapshot(skv.CommitedId, data)
		debug(dInfo, "G%v S%v Snapshot Index %v, kv.rf.RaftStateSize: %v", skv.gid, skv.me, skv.CommitedId, skv.rf.GetRaftStateSize())
		skv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) applyLog() {
	for {
		applyMsg := <-skv.applyCh
		skv.mu.Lock()
		if applyMsg.CommandValid {
			msg := applyMsg.Command.(Op)
			// debug(dInfo, "SK%v receive clientId: %v, reqNo: %v, index: %v", skv.me, msg.ClientId, msg.ReqNo, applyMsg.CommandIndex)
			if msg.ReqNo > skv.CompletedPool[msg.ClientId] {
				result := skv.doOperation(msg)
				// debug(dInfo, "G%v S%v apply Index %v clientId: %v msgReqNo: %v",
				// 	skv.gid, skv.me, applyMsg.CommandIndex, msg.ClientId, msg.ReqNo)
				skv.CompletedPool[msg.ClientId] = msg.ReqNo
				skv.CompletedResult[msg.ClientId] = result
			}
			if _, exist := skv.CompletedCondPool[msg.ClientId]; exist {
				skv.CompletedCondPool[msg.ClientId].Broadcast()
			}
			skv.CommitedId = max(skv.CommitedId, applyMsg.CommandIndex)
		} else if applyMsg.SnapshotIndex > skv.CommitedId {
			skv.ReadSnapshot(applyMsg.Snapshot)
		}
		skv.mu.Unlock()
	}
}

func (skv *ShardKV) doOperation(opMsg Op) Result {
	result := Result{}
	result.Err = OK
	switch opMsg.OpType {
	case GET:
		if _, exist := skv.Shards[opMsg.GetArg.ShardId]; !exist || !skv.Shards[opMsg.GetArg.ShardId] {
			result.Err = ErrWrongGroup
		} else {
			result.Value = skv.ShardKVMap[opMsg.GetArg.Key]
		}
	case PUT:
		if _, exist := skv.Shards[opMsg.PutAppendArg.ShardId]; !exist || !skv.Shards[opMsg.PutAppendArg.ShardId] {
			// debug(dInfo, "G%v S%v key: %v value: %v shard: %v Shards: %v config: %v",
			// 	skv.gid, skv.me, opMsg.PutAppendArg.Key, opMsg.PutAppendArg.Value, opMsg.PutAppendArg.ShardId, skv.Shards, skv.Config)
			result.Err = ErrWrongGroup
		} else {
			skv.ShardKVMap[opMsg.PutAppendArg.Key] = opMsg.PutAppendArg.Value
			result.Value = skv.ShardKVMap[opMsg.PutAppendArg.Key]
		}
	case APPEND:
		if _, exist := skv.Shards[opMsg.GetArg.ShardId]; !exist || !skv.Shards[opMsg.GetArg.ShardId] {
			result.Err = ErrWrongGroup
		} else {
			skv.ShardKVMap[opMsg.PutAppendArg.Key] += opMsg.PutAppendArg.Value
			result.Value = skv.ShardKVMap[opMsg.PutAppendArg.Key]
		}
	case CONFIG:
		skv.doUpdateConfig(opMsg.KvState)
	}
	return result
}

func (skv *ShardKV) doUpdateConfig(kvState KVState) {
	if kvState.Config.Num <= skv.Config.Num {
		return
	}
	ShardIds := make(map[int]bool)
	for shard, gid := range kvState.Config.Shards {
		if gid == skv.gid {
			ShardIds[shard] = true
		}
	}
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if _, ok := kvState.KeepShards[shard]; ok && kvState.KeepShards[shard] {
			kvState.KVMap[key] = value
		}
	}
	skv.Shards = ShardIds
	skv.ShardKVMap = kvState.KVMap
	skv.Config = kvState.Config
	debug(dInfo, "G%v S%v update ConfigNum %v Shards %v KVMap %v", skv.gid, skv.me, skv.Config.Num, skv.Shards, skv.ShardKVMap)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
