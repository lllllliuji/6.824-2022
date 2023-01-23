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
	SHARDS OpType = "SHARDS"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       OpType
	GetArg       GetArgs
	PutAppendArg PutAppendArgs
	ClientId     int64
	ReqNo        int64
	Config       shardctrler.Config
	ShiftShards  []ShiftShard
	ClientInfo   ClientInfo
}

type Result struct {
	Err      Err
	Value    string
	ClientId int64
	ReqNo    int64
}

type ShiftShard struct {
	ConfigNum int
	ShardId   int
	KVMap     map[string]string
}

func (ssd *ShiftShard) copy() ShiftShard {
	retShiftShard := ShiftShard{}
	retShiftShard.ConfigNum = ssd.ConfigNum
	retShiftShard.ShardId = ssd.ShardId
	retShiftShard.KVMap = make(map[string]string)
	for key, value := range ssd.KVMap {
		retShiftShard.KVMap[key] = value
	}
	return retShiftShard
}

type ClientInfo struct {
	CompletedReq    map[int64]int64
	CompletedResult map[int64]Result
}

func (cInfo *ClientInfo) copy() ClientInfo {
	retInfo := ClientInfo{}
	retInfo.CompletedReq = make(map[int64]int64)
	retInfo.CompletedResult = make(map[int64]Result)
	for clientId, req := range cInfo.CompletedReq {
		retInfo.CompletedReq[clientId] = req
	}
	for clientId, result := range cInfo.CompletedResult {
		retInfo.CompletedResult[clientId] = result
	}
	return retInfo
}

func (cInfo *ClientInfo) merge(info ClientInfo) {
	for clientId, req := range info.CompletedReq {
		if req > cInfo.CompletedReq[clientId] {
			cInfo.CompletedReq[clientId] = req
			cInfo.CompletedResult[clientId] = info.CompletedResult[clientId]
		} else if req == cInfo.CompletedReq[clientId] && cInfo.CompletedResult[clientId].Err != OK { // succesfull operation may be overwrite, carefull
			cInfo.CompletedResult[clientId] = info.CompletedResult[clientId]
		}
	}
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

	CommitedId               int
	LastConfig               shardctrler.Config
	CurConfig                shardctrler.Config
	Shards                   map[int]bool
	ShardKVMap               map[string]string
	ClientInfo               ClientInfo
	CompletedChan            map[int64]chan Result
	Gid2UnCompletedConfigNum map[int]int

	// need garbage collection
	OutShardsDock     map[int]map[int]ShiftShard // configNum -> shardId -> shard data
	Config2ClientInfo map[int]ClientInfo

	Dead int32
}

func (skv *ShardKV) Hello(args *HelloArgs, reply *HelloReply) {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	if completed, _ := skv.isMigrateFinished(); completed {
		reply.ConfigNum = skv.CurConfig.Num + 1
	} else {
		reply.ConfigNum = skv.CurConfig.Num
	}
	reply.Success = true
}

func (skv *ShardKV) sayHello() {
	for !skv.killed() {
		skv.mu.Lock()
		gid2ConfigNum := make(map[int]int)
		for gid, configNum := range skv.Gid2UnCompletedConfigNum {
			gid2ConfigNum[gid] = configNum
		}
		curConfig := skv.CurConfig
		skv.mu.Unlock()
		for gid, servers := range curConfig.Groups {
			if gid == skv.gid {
				continue
			}
			for _, server := range servers {
				args := HelloArgs{}
				reply := HelloReply{}
				srv := skv.make_end(server)
				ok := srv.Call("ShardKV.Hello", &args, &reply)
				if ok && reply.Success {
					gid2ConfigNum[gid] = max(gid2ConfigNum[gid], reply.ConfigNum)
				}
			}
		}
		skv.mu.Lock()
		for gid, configNum := range gid2ConfigNum {
			skv.Gid2UnCompletedConfigNum[gid] = max(skv.Gid2UnCompletedConfigNum[gid], configNum)
		}
		debug(dInfo, "G%v S%v gid2UnCompletedConfigNum %v", skv.gid, skv.me, skv.Gid2UnCompletedConfigNum)
		if len(skv.Gid2UnCompletedConfigNum) > 1 {
			skv.clean()
		}
		skv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) clean() {
	lowestConfigNum := 0
	for _, configNum := range skv.Gid2UnCompletedConfigNum {
		if lowestConfigNum == 0 {
			lowestConfigNum = configNum
		} else {
			lowestConfigNum = min(lowestConfigNum, configNum)
		}
	}
	expiredClientInfoConfigNum := make(map[int]bool)
	expiredOutShardConfigNum := make(map[int]bool)
	for configNum := range skv.Config2ClientInfo {
		if configNum < lowestConfigNum {
			expiredClientInfoConfigNum[configNum] = true
		}
	}
	for configNum := range skv.OutShardsDock {
		if configNum < lowestConfigNum {
			expiredOutShardConfigNum[configNum] = true
		}
	}
	for configNum := range expiredClientInfoConfigNum {
		delete(skv.Config2ClientInfo, configNum)
	}
	for configNum := range expiredOutShardConfigNum {
		delete(skv.OutShardsDock, configNum)
	}
	debug(dInfo, "G%v S%v delete to configNum %v", skv.gid, skv.me, lowestConfigNum)
}

func (skv *ShardKV) isMigrateFinished() (bool, []int) {
	finished := true
	unFinishedShard := []int{}
	for shard, gid := range skv.CurConfig.Shards {
		if skv.gid == gid && !skv.Shards[shard] {
			finished = false
			unFinishedShard = append(unFinishedShard, shard)
		}
	}
	return finished, unFinishedShard
}

func (skv *ShardKV) updateConfig() {
	for !skv.killed() {
		for !skv.killed() {
			skv.mu.Lock()
			if _, isLeader := skv.rf.GetState(); !isLeader {
				skv.mu.Unlock()
				break
			}
			if finished, _ := skv.isMigrateFinished(); !finished {
				skv.mu.Unlock()
				break
			}
			curConfig := skv.CurConfig
			nextConfig := skv.mck.Query(curConfig.Num + 1)
			if nextConfig.Num <= curConfig.Num {
				skv.mu.Unlock()
				break
			}
			op := Op{
				OpType: CONFIG,
				Config: nextConfig,
			}
			skv.rf.Start(op)
			skv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) ShardDock(args *PullShardsArgs, reply *PullShardReply) {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, exist := skv.OutShardsDock[args.ConfigNum]; !exist {
		reply.Success = false
		return
	}
	unPrepared := true
	retShards := []ShiftShard{}
	for _, shard := range args.ShardIds {
		if outShard, exist := skv.OutShardsDock[args.ConfigNum][shard]; exist {
			unPrepared = false
			retShards = append(retShards, outShard.copy())
		}
	}
	if unPrepared {
		reply.Success = false
		return
	}
	reply.Success = true
	reply.ShiftShards = retShards
	reply.ClientInfo = skv.Config2ClientInfo[args.ConfigNum]
	debug(dInfo, "G%v S%v ShardDock %v shard %v clientInfo %v", skv.gid, skv.me, args.ConfigNum, retShards, reply.ClientInfo)
}

func (skv *ShardKV) doPullShards(servers []string, configNum int, unFinishedShard []int) {
	if len(servers) == 0 {
		return
	}
	if len(unFinishedShard) == 0 {
		return
	}
	for {
		for _, server := range servers {
			args := PullShardsArgs{ConfigNum: configNum, ShardIds: unFinishedShard}
			reply := PullShardReply{}
			srv := skv.make_end(server)
			ok := srv.Call("ShardKV.ShardDock", &args, &reply)
			if ok && reply.Success {
				op := Op{
					OpType:      SHARDS,
					ShiftShards: reply.ShiftShards,
					ClientInfo:  reply.ClientInfo,
				}
				go skv.rf.Start(op)
				return
			}
		}
		skv.mu.Lock()
		if skv.CurConfig.Num != configNum {
			skv.mu.Unlock()
			return
		}
		skv.mu.Unlock()
		// target server may haven't prepare neccessary data, wait a moment
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) pullShards(lastConfig, curConfig shardctrler.Config, unFInishedShard []int) {
	// first config, nowhere to pull
	debug(dInfo, "G%v S%v pullShards configNum %v unFinnishedShard %v", skv.gid, skv.me, curConfig.Num, unFInishedShard)
	if lastConfig.Shards[0] == 0 {
		shiftShards := []ShiftShard{}
		for shard := range curConfig.Shards {
			shiftShards = append(shiftShards, ShiftShard{ConfigNum: curConfig.Num, ShardId: shard, KVMap: make(map[string]string)})
		}
		op := Op{
			OpType:      SHARDS,
			ShiftShards: shiftShards,
		}
		go skv.rf.Start(op)
		return
	}
	targetGids := make(map[int]bool)
	gid2Shards := make(map[int][]int)
	for _, shard := range unFInishedShard {
		gid := lastConfig.Shards[shard]
		targetGids[gid] = true
		if gid2Shards[gid] == nil {
			gid2Shards[gid] = []int{}
		}
		gid2Shards[gid] = append(gid2Shards[gid], shard)
	}
	for gid := range targetGids {
		go skv.doPullShards(lastConfig.Groups[gid], curConfig.Num, gid2Shards[gid])
	}
}

func (skv *ShardKV) checkMissShards() {
	for !skv.killed() {
		for !skv.killed() {
			skv.mu.Lock()
			if _, isLeader := skv.rf.GetState(); !isLeader {
				skv.mu.Unlock()
				break
			}
			finished, unFinishedShards := skv.isMigrateFinished()
			if finished {
				skv.mu.Unlock()
				break
			}
			lastConfig := skv.LastConfig
			curConfig := skv.CurConfig
			skv.mu.Unlock()
			skv.pullShards(lastConfig, curConfig, unFinishedShards)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// before call this function, check isMigrateFinished
func (skv *ShardKV) makeOutShards(outShards map[int]bool, nextConfig shardctrler.Config) {
	outShardsMap := make(map[int]ShiftShard) // shardId -> shardMap
	for shard := range outShards {
		outShardsMap[shard] = ShiftShard{ConfigNum: nextConfig.Num, ShardId: shard, KVMap: make(map[string]string)}
	}
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if outShards[shard] {
			outShardsMap[shard].KVMap[key] = value
		}
	}
	skv.OutShardsDock[nextConfig.Num] = outShardsMap // curConfig out, belongs to nextConfig
	debug(dInfo, "G%v S%v curConfig %v nextConfig %v make OutShardsMap: %v", skv.gid, skv.me, skv.CurConfig.Num, nextConfig.Num, skv.OutShardsDock[nextConfig.Num])
}

func (skv *ShardKV) compareConfig(nextConfig shardctrler.Config) (map[int]bool, map[int]bool) {
	outShards := make(map[int]bool)
	keepShards := make(map[int]bool)
	for shard, gid := range nextConfig.Shards {
		// migrate out
		if skv.Shards[shard] && skv.gid != gid {
			outShards[shard] = true
		}
		// keep as normal
		if skv.Shards[shard] && skv.gid == gid {
			keepShards[shard] = true
		}
	}
	return outShards, keepShards
}

func (skv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if shard := key2shard(args.Key); skv.CurConfig.Shards[shard] != skv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if args.ReqNo < skv.ClientInfo.CompletedReq[args.ClientId] ||
		(args.ReqNo == skv.ClientInfo.CompletedReq[args.ClientId] && skv.ClientInfo.CompletedResult[args.ClientId].Err == OK) {
		if !skv.Shards[key2shard(args.Key)] {
			reply.Err = ErrNotReady
		} else {
			reply.Err = OK
			reply.Value = skv.ShardKVMap[args.Key]
		}
		debug(dInfo, "G%v S%v receive duplicated Get request ReqNo: %v CompletedReqNo: %v from C%v",
			skv.gid, skv.me, args.ReqNo, skv.ClientInfo.CompletedReq[args.ClientId], args.ClientId)
		return
	}
	op := Op{
		OpType:   GET,
		GetArg:   *args,
		ClientId: args.ClientId,
		ReqNo:    args.ReqNo,
	}
	index, _, isLeader := skv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	debug(dInfo, "G%v S%v receive Get Request Get %v Shard: %v, from C%v ReqNo %v CompletedReqNo %v index %v",
		skv.gid, skv.me, args.Key, key2shard(args.Key), args.ClientId, args.ReqNo, skv.ClientInfo.CompletedReq[args.ClientId], index)
	skv.CompletedChan[args.ClientId] = make(chan Result)
	ch := skv.CompletedChan[args.ClientId]
	skv.mu.Unlock()
	select {
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeOut
	case result := <-ch:
		if result.ClientId == args.ClientId && result.ReqNo == args.ReqNo {
			reply.Err = result.Err
			reply.Value = result.Value
		}
	}
	skv.mu.Lock()
	delete(skv.CompletedChan, args.ClientId)
	debug(dInfo, "G%v S%v complete Get Request %v %v Shard %v index %v Err %v from C%v Shards %v ConfigShard %v Config2ClientInfo %v OutshardDock %v",
		skv.gid, skv.me, args.Key, reply.Value, key2shard(args.Key), index, reply.Err, args.ClientId, skv.Shards, skv.CurConfig.Shards, skv.Config2ClientInfo, skv.OutShardsDock)
}

func (skv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	skv.mu.Lock()
	defer skv.mu.Unlock()
	if _, isLeader := skv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if shard := key2shard(args.Key); skv.CurConfig.Shards[shard] != skv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if args.ReqNo < skv.ClientInfo.CompletedReq[args.ClientId] ||
		(args.ReqNo == skv.ClientInfo.CompletedReq[args.ClientId] && skv.ClientInfo.CompletedResult[args.ClientId].Err == OK) {
		reply.Err = OK
		debug(dInfo, "G%v S%v receive duplicated %v request ReqNo: %v CompletedReqNo: %v from C%v",
			skv.gid, skv.me, args.Op, args.ReqNo, skv.ClientInfo.CompletedReq[args.ClientId], args.ClientId)
		return
	}
	if args.ReqNo == skv.ClientInfo.CompletedReq[args.ClientId] {
		debug(dInfo, "G%v S%v duplicate request %v from C%v LastErr %v", skv.gid, skv.me, args.ReqNo, args.ClientId, skv.ClientInfo.CompletedResult[args.ClientId].Err)
	}
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
	index, _, isLeader := skv.rf.Start(op)
	debug(dInfo, "G%v S%v receive %v Request [%v, %v] Shard %v from C%v ReqNo %v CompletedReq %v index %v",
		skv.gid, skv.me, args.Op, args.Key, args.Value, args.ShardId, args.ClientId, args.ReqNo, skv.ClientInfo.CompletedReq[args.ClientId], index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	skv.CompletedChan[args.ClientId] = make(chan Result)
	ch := skv.CompletedChan[args.ClientId]
	skv.mu.Unlock()
	select {
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeOut
	case result := <-ch:
		if result.ClientId == args.ClientId && result.ReqNo == args.ReqNo {
			reply.Err = result.Err
		}
	}
	skv.mu.Lock()
	delete(skv.CompletedChan, args.ClientId)
	debug(dInfo, "G%v S%v complete %v Request [%v, %v] Result %v Shard %v from C%v ReqNo %v index %v Err %v Shards %v ConfigShard %v",
		skv.gid, skv.me, args.Op, args.Key, args.Value, skv.ShardKVMap[args.Key], args.ShardId, args.ClientId, args.ReqNo, index, reply.Err, skv.Shards, skv.CurConfig.Shards)
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
	atomic.StoreInt32(&skv.Dead, 1)
}

func (skv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&skv.Dead)
	return z == 1
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
	labgob.Register(ShiftShard{})
	labgob.Register(ClientInfo{})

	skv := new(ShardKV)
	skv.me = me
	skv.maxraftstate = maxraftstate
	skv.make_end = make_end
	skv.gid = gid
	skv.ctrlers = ctrlers

	// Your initialization code here.
	skv.LastConfig = shardctrler.Config{}
	skv.CurConfig = shardctrler.Config{}
	skv.Shards = make(map[int]bool)
	skv.ShardKVMap = make(map[string]string)

	skv.ClientInfo = ClientInfo{CompletedReq: make(map[int64]int64), CompletedResult: make(map[int64]Result)}

	skv.CommitedId = 0
	skv.CompletedChan = make(map[int64]chan Result)

	// need garbage collection
	skv.OutShardsDock = make(map[int]map[int]ShiftShard)
	skv.Config2ClientInfo = make(map[int]ClientInfo)

	skv.Gid2UnCompletedConfigNum = make(map[int]int)
	skv.mck = shardctrler.MakeClerk(ctrlers)
	skv.Dead = 0

	// debug(dInfo, "G%v S%v len(raftstate): %v", skv.gid, skv.me, len(persister.ReadRaftState()))
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	skv.applyCh = make(chan raft.ApplyMsg)
	skv.rf = raft.Make(servers, me, persister, skv.applyCh)
	skv.ReadSnapshot(persister.ReadSnapshot())

	go skv.Snapshot()
	go skv.applyLog()
	go skv.updateConfig()
	go skv.checkMissShards()
	go skv.sayHello()
	// go skv.cleaner()
	debug(dInfo, "G%v S%v start", skv.gid, skv.me)
	return skv
}

func (skv *ShardKV) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CommitedId int
	var LastConfig shardctrler.Config
	var CurConfig shardctrler.Config
	var Shards map[int]bool
	var ShardKVMap map[string]string
	var ClientInfos ClientInfo
	var Gid2UnCompletedConfigNum map[int]int
	var OutShardsDock map[int]map[int]ShiftShard
	var Config2ClientInfo map[int]ClientInfo
	if d.Decode(&CommitedId) != nil || d.Decode(&LastConfig) != nil || d.Decode(&CurConfig) != nil || d.Decode(&Shards) != nil ||
		d.Decode(&ShardKVMap) != nil || d.Decode(&ClientInfos) != nil || d.Decode(&Gid2UnCompletedConfigNum) != nil || d.Decode(&OutShardsDock) != nil || d.Decode(&Config2ClientInfo) != nil {
		debug(dError, "G%v S%v Error when decode in ReadSnapshot", skv.gid, skv.me)
		return
	} else {
		skv.CommitedId = CommitedId
		skv.LastConfig = LastConfig
		skv.CurConfig = CurConfig
		skv.Shards = Shards
		skv.ShardKVMap = ShardKVMap
		skv.ClientInfo = ClientInfos
		skv.Gid2UnCompletedConfigNum = Gid2UnCompletedConfigNum
		skv.OutShardsDock = OutShardsDock
		skv.Config2ClientInfo = Config2ClientInfo
	}
	skv.rf.Snapshot(skv.CommitedId, data)
	debug(dInfo, "G%v S%v ReadSnapshot Index: %v ConfigNum %v ShardKVMap %v", skv.gid, skv.me, CommitedId, skv.CurConfig.Num, skv.ShardKVMap)
}

func (skv *ShardKV) Snapshot() {
	for !skv.killed() {
		skv.mu.Lock()
		if skv.maxraftstate == -1 || skv.rf.GetRaftStateSize() < skv.maxraftstate {
			skv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(skv.CommitedId)
		e.Encode(skv.LastConfig)
		e.Encode(skv.CurConfig)
		e.Encode(skv.Shards)
		e.Encode(skv.ShardKVMap)
		e.Encode(skv.ClientInfo)
		e.Encode(skv.Gid2UnCompletedConfigNum)
		e.Encode(skv.OutShardsDock)
		e.Encode(skv.Config2ClientInfo)
		data := w.Bytes()
		skv.rf.Snapshot(skv.CommitedId, data)
		debug(dInfo, "G%v S%v Snapshot Index %v, kv.rf.RaftStateSize: %v SnapshotSize: %v ConfigNum %v", skv.gid, skv.me, skv.CommitedId, skv.rf.GetRaftStateSize(), len(data), skv.CurConfig.Num)
		skv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (skv *ShardKV) applyLog() {
	for !skv.killed() {
		applyMsg := <-skv.applyCh
		skv.mu.Lock()
		debug(dInfo, "G%v S%v receive commandIndex %v, snapshotIndex %v", skv.gid, skv.me, applyMsg.CommandIndex, applyMsg.SnapshotIndex)
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			debug(dInfo, "G%v S%v receive clientId: %v, reqNo: %v, index: %v applyMsg %v", skv.gid, skv.me, op.ClientId, op.ReqNo, applyMsg.CommandIndex, applyMsg)
			switch op.OpType {
			case GET:
				if op.ReqNo > skv.ClientInfo.CompletedReq[op.ClientId] ||
					(op.ReqNo == skv.ClientInfo.CompletedReq[op.ClientId] && skv.ClientInfo.CompletedResult[op.ClientId].Err != OK) {
					skv.doOperation(op, applyMsg.CommandIndex)
				}
			case PUT:
				if op.ReqNo > skv.ClientInfo.CompletedReq[op.ClientId] ||
					(op.ReqNo == skv.ClientInfo.CompletedReq[op.ClientId] && skv.ClientInfo.CompletedResult[op.ClientId].Err != OK) {
					skv.doOperation(op, applyMsg.CommandIndex)
				}
			case APPEND:
				if op.ReqNo > skv.ClientInfo.CompletedReq[op.ClientId] ||
					(op.ReqNo == skv.ClientInfo.CompletedReq[op.ClientId] && skv.ClientInfo.CompletedResult[op.ClientId].Err != OK) {
					skv.doOperation(op, applyMsg.CommandIndex)
				}
			case CONFIG:
				skv.doUpdateConfig(op.Config)
				debug(dInfo, "G%v S%v apply Config index %v err %v", skv.gid, skv.me, applyMsg.CommandIndex, skv.ClientInfo.CompletedResult[op.ClientId].Err)
			case SHARDS:
				skv.acceptShards(op)
				debug(dInfo, "G%v S%v apply Shards index %v err %v", skv.gid, skv.me, applyMsg.CommandIndex, skv.ClientInfo.CompletedResult[op.ClientId].Err)
			}
			skv.CommitedId = max(skv.CommitedId, applyMsg.CommandIndex)
		} else if applyMsg.SnapshotValid && applyMsg.SnapshotIndex > skv.CommitedId {
			skv.ReadSnapshot(applyMsg.Snapshot)
		}
		skv.mu.Unlock()
	}
}

func (skv *ShardKV) doOperation(op Op, index int) {
	result := Result{
		Err:      OK,
		ClientId: op.ClientId,
		ReqNo:    op.ReqNo,
	}
	switch op.OpType {
	case GET:
		if skv.CurConfig.Shards[op.GetArg.ShardId] != skv.gid {
			result.Err = ErrWrongGroup
		} else if !skv.Shards[op.GetArg.ShardId] {
			result.Err = ErrNotReady
		} else {
			result.Value = skv.ShardKVMap[op.GetArg.Key]
		}
	case PUT:
		if skv.CurConfig.Shards[op.PutAppendArg.ShardId] != skv.gid {
			result.Err = ErrWrongGroup
		} else if !skv.Shards[op.PutAppendArg.ShardId] {
			result.Err = ErrNotReady
		} else {
			skv.ShardKVMap[op.PutAppendArg.Key] = op.PutAppendArg.Value
		}
	case APPEND:
		if skv.CurConfig.Shards[op.PutAppendArg.ShardId] != skv.gid {
			result.Err = ErrWrongGroup
		} else if !skv.Shards[op.PutAppendArg.ShardId] {
			result.Err = ErrNotReady
		} else {
			skv.ShardKVMap[op.PutAppendArg.Key] += op.PutAppendArg.Value
		}
	}
	if _, exist := skv.CompletedChan[op.ClientId]; exist {
		go func(ch chan Result, res Result) {
			ch <- res
		}(skv.CompletedChan[op.ClientId], result)
	}
	skv.ClientInfo.CompletedReq[op.ClientId] = op.ReqNo
	skv.ClientInfo.CompletedResult[op.ClientId] = Result{Err: result.Err}
	debug(dInfo, "G%v S%v apply %v Request ClientId %v index %v Result %v", skv.gid, skv.me, op.OpType, op.ClientId, index, result)
}

func (skv *ShardKV) acceptShards(op Op) {
	shiftShards := op.ShiftShards
	for _, shiftShard := range shiftShards {
		if shiftShard.ConfigNum != skv.CurConfig.Num || skv.CurConfig.Shards[shiftShard.ShardId] != skv.gid || skv.Shards[shiftShard.ShardId] {
			continue
		}
		for key, value := range shiftShard.KVMap {
			skv.ShardKVMap[key] = value
		}
		if skv.CurConfig.Shards[shiftShard.ShardId] == skv.gid {
			skv.Shards[shiftShard.ShardId] = true
		}
	}
	skv.ClientInfo.merge(op.ClientInfo)
	debug(dInfo, "G%v S%v configNum %v acceptShards skv.Shards %v shiftShards %v clientInfo %v ShardKV %v", skv.gid, skv.me, skv.CurConfig.Num, skv.Shards, shiftShards, op.ClientInfo, skv.ShardKVMap)
}

func (skv *ShardKV) doUpdateConfig(nextConfig shardctrler.Config) {
	if nextConfig.Num <= skv.CurConfig.Num {
		return
	}
	if finished, _ := skv.isMigrateFinished(); !finished {
		return
	}
	outShards, keepShards := skv.compareConfig(nextConfig)
	skv.Config2ClientInfo[nextConfig.Num] = skv.ClientInfo.copy()
	// debug(dInfo, "G%v S%v curConfig %v nextConfig %v make OutShardClientInfo: %v", skv.gid, skv.me, skv.CurConfig.Num, nextConfig.Num, skv.Config2ClientInfo[nextConfig.Num])
	skv.makeOutShards(outShards, nextConfig)
	keepKVMap := make(map[string]string)
	for key, value := range skv.ShardKVMap {
		shard := key2shard(key)
		if keepShards[shard] {
			keepKVMap[key] = value
		}
	}
	skv.Shards = keepShards
	skv.ShardKVMap = keepKVMap
	skv.LastConfig = skv.CurConfig
	skv.CurConfig = nextConfig
	debug(dInfo, "G%v S%v update ConfigNum %v Shards %v KVMap %v ConfigShards %v", skv.gid, skv.me, skv.CurConfig.Num, skv.Shards, skv.ShardKVMap, skv.CurConfig.Shards)
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
