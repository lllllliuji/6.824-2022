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

type ShiftShards struct {
	Gid   int
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

	ConfigReqNo int64
	
	Gid2ShardsBuffer map[int]map[int]ShiftShards
	Gid2ConfigNum    map[int]int
}

func (skv *ShardKV) cleaner() {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	lowestConfigNum := skv.Config.Num
	for _, configNum := range skv.Gid2ConfigNum {
		lowestConfigNum = min(lowestConfigNum, configNum)
	}
	expiredConfigNum := make(map[int]bool)
	for configNum := range skv.Gid2ShardsBuffer {
		if configNum <= lowestConfigNum {
			expiredConfigNum[configNum] = true
		}
	}
	for configNum := range expiredConfigNum {
		delete(skv.Gid2ShardsBuffer, configNum)
	}
}

func (skv *ShardKV) Hello(args *HelloArgs, reply *HelloReply) {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	reply.ConfigNum = skv.Config.Num
	reply.Success = true
}

func (skv *ShardKV) sayHelloToEveryGroup() {
	for {
		skv.mu.Lock()
		curConfig := skv.Config
		skv.mu.Unlock()
		gid2configNum := make(map[int]int)
		for gid, servers := range curConfig.Groups {
			if gid == skv.gid {
				continue
			}
			args := HelloArgs{Gid: skv.gid, ServerId: skv.me}
			reply := HelloReply{}
			for _, server := range servers {
				srv := skv.make_end(server)
				ok := srv.Call("ShardKV.Hello", &args, &reply)
				if ok && reply.Success {
					gid2configNum[gid] = reply.ConfigNum
					break
				}
			}
		}
		skv.mu.Lock()
		skv.Gid2ConfigNum = gid2configNum
		skv.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

func (skv *ShardKV) updateConfig() {
	for {
		for {
			if _, isLeader := skv.rf.GetState(); !isLeader {
				break
			}
			skv.mu.Lock()
			curConfig := skv.Config
			nextConfig := skv.mck.Query(curConfig.Num + 1)
			if curConfig.Num == nextConfig.Num {
				skv.mu.Unlock()
				break
			}
			inShards, outShards, keepShards := skv.compareConfig(nextConfig)
			skv.migrateOutShards(outShards, curConfig, nextConfig)
			skv.mu.Unlock()
			skv.makeConfigAgreement(inShards, keepShards, curConfig, nextConfig)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) MigrateShards(args *MigrateShardArgs, reply *MigrateShardReply) {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, exist := skv.Gid2ShardsBuffer[args.ConfigNum]; !exist {
		reply.Success = false
		return
	}
	if _, exist := skv.Gid2ShardsBuffer[args.ConfigNum][args.Gid]; !exist {
		reply.Success = false
		return
	}
	retMap := make(map[string]string)
	for key, value := range skv.Gid2ShardsBuffer[args.ConfigNum][args.Gid].KVMap {
		retMap[key] = value
	}
	reply.KVMap = retMap
	reply.Success = true
	debug(dInfo, "G%v S%v migrate shards from G%v S%v ShardsMap: %v", args.Gid, args.ServerId, skv.gid, skv.me, retMap)
}

func (skv *ShardKV) makeInShardsMap(inShards map[int]bool, curConfig, nextConfig shardctrler.Config) map[string]string {
	retMap := make(map[string]string)
	gids := make(map[int]bool)
	for shard := range inShards {
		gid := curConfig.Shards[shard]
		gids[gid] = true
	}
	for gid := range gids {
		if servers, exist := curConfig.Groups[gid]; exist {
			args := MigrateShardArgs{Gid: skv.gid, ConfigNum: nextConfig.Num, ServerId: skv.me}
			reply := MigrateShardReply{}
			for {
				retry := true
				for _, server := range servers {
					srv := skv.make_end(server)
					ok := srv.Call("ShardKV.MigrateShards", &args, &reply)
					if ok && reply.Success {
						retry = false
						break
					}
				}
				if !retry {
					break
				}
			}
			for key, value := range reply.KVMap {
				retMap[key] = value
			}
		}
	}
	debug(dInfo, "G%v S%v curConfig: %v nextConfig: %v makeNewMap: %v inShards: %v",
		skv.gid, skv.me, curConfig.Num, nextConfig.Num, retMap, inShards)
	return retMap
}

func (skv *ShardKV) makeConfigAgreement(inShards, keepShards map[int]bool, curConfig, nextConfig shardctrler.Config) {
	op := Op{
		OpType:   CONFIG,
		ClientId: 0,
		ReqNo:    atomic.AddInt64(&skv.ConfigReqNo, 1),
		Config:   nextConfig,
		KvState:  KVState{KVMap: skv.makeInShardsMap(inShards, curConfig, nextConfig), Config: nextConfig, KeepShards: keepShards},
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

func (skv *ShardKV) migrateOutShards(outShards map[int]bool, curConfig, nextConfig shardctrler.Config) {
	gid2Shards := make(map[int]map[string]string)
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if outShards[shard] {
			gid := nextConfig.Shards[shard]
			if gid2Shards[gid] == nil {
				gid2Shards[gid] = make(map[string]string)
			}
			gid2Shards[gid][key] = value
			// stop serve outshards
			skv.Shards[shard] = false
		}
	}
	if skv.Gid2ShardsBuffer[nextConfig.Num] == nil {
		skv.Gid2ShardsBuffer[nextConfig.Num] = make(map[int]ShiftShards)
	}
	for gid, KVMap := range gid2Shards {
		skv.Gid2ShardsBuffer[nextConfig.Num][gid] = ShiftShards{Gid: gid, KVMap: KVMap}
	}
	// if outshards has no key value in skv.ShardKVMap, at least a empty map
	gids := make(map[int]bool)
	for shard := range outShards {
		gids[nextConfig.Shards[shard]] = true
	}
	for gid := range gids {
		if _, exist := skv.Gid2ShardsBuffer[nextConfig.Num][gid]; !exist {
			skv.Gid2ShardsBuffer[nextConfig.Num][gid] = ShiftShards{}
		}
	}
	debug(dInfo, "G%v S%v curConfig: %v nextConfig: %v OutShardsMap: %v", skv.gid, skv.me, curConfig.Num, nextConfig.Num, skv.Gid2ShardsBuffer[nextConfig.Num])
}

func (skv *ShardKV) compareConfig(nextConfig shardctrler.Config) (map[int]bool, map[int]bool, map[int]bool) {
	inShards := make(map[int]bool)
	outShards := make(map[int]bool)
	keepShards := make(map[int]bool)
	for shard, gid := range nextConfig.Shards {
		// migrate out
		if skv.Shards[shard] && skv.gid != gid {
			outShards[shard] = true
		}
		// migrate in
		if !skv.Shards[shard] && skv.gid == gid {
			inShards[shard] = true
		}
		// keep as normal
		if skv.Shards[shard] && skv.gid == gid {
			keepShards[shard] = true
		}
	}
	return inShards, outShards, keepShards
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
	labgob.Register(ShiftShards{})

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
	skv.Gid2ShardsBuffer = make(map[int]map[int]ShiftShards)
	skv.Gid2ConfigNum = make(map[int]int)
	// skv.IncomeShards = make(map[int]map[int]ShiftShard)
	// skv.OutcomeShards = make(map[int]map[int]ShiftShard)
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
	go skv.sayHelloToEveryGroup()
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
	var Gid2ShardsBuffer map[int]map[int]ShiftShards

	if d.Decode(&Config) != nil || d.Decode(&CommitedId) != nil || d.Decode(&ConfigReqNo) != nil || d.Decode(&ShardIds) != nil ||
		d.Decode(&ShardKVMap) != nil || d.Decode(&CompletedPool) != nil || d.Decode(&CompletedResult) != nil || d.Decode(&Gid2ShardsBuffer) != nil {
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
		skv.Gid2ShardsBuffer = Gid2ShardsBuffer
	}
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
		e.Encode(skv.Gid2ShardsBuffer)
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
		if _, exist := skv.Shards[opMsg.PutAppendArg.ShardId]; !exist || !skv.Shards[opMsg.PutAppendArg.ShardId] {
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
	KVMap := make(map[string]string)
	for key, value := range kvState.KVMap {
		KVMap[key] = value
	}
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if _, ok := kvState.KeepShards[shard]; ok && kvState.KeepShards[shard] {
			KVMap[key] = value
		}
	}
	skv.Shards = ShardIds
	skv.ShardKVMap = KVMap
	skv.Config = kvState.Config
	debug(dInfo, "G%v S%v update ConfigNum %v Shards %v KVMap %v", skv.gid, skv.me, skv.Config.Num, skv.Shards, skv.ShardKVMap)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
