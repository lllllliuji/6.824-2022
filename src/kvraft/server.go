package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IsGetOp      bool
	GetArg       GetArgs
	PutAppendArg PutAppendArgs
	ClientId     int64
	ReqNo        int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVMap             map[string]string
	CompletedPool     map[int64]int64
	CompletedCondPool map[int64]*sync.Cond
	CommitedId        int
	// SnapshotCond      *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// debug(dInfo, "K%v GetLock", kv.me)
	defer func() {
		kv.mu.Unlock()
		// debug(dInfo, "K%v GetUnlock", kv.me)
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	if args.ReqNo <= kv.CompletedPool[args.ClientId] {
		reply.Success = true
		reply.Value = kv.KVMap[args.Key]
		debug(dInfo, "K%v receive duplicated request from C%v", kv.me, args.ClientId)
		return
	}
	debug(dInfo, "K%v receive Get Request Get %v, from C%v", kv.me, args.Key, args.ClientId)
	op := Op{
		IsGetOp:  true,
		GetArg:   *args,
		ClientId: args.ClientId,
		ReqNo:    args.ReqNo,
	}
	index, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		return
	}
	kv.CompletedCondPool[args.ClientId] = sync.NewCond(&kv.mu)
	for {
		kv.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := kv.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.Success = false
			return
		}
		if completedReq, exist := kv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
	reply.Success = true
	reply.Value = kv.KVMap[args.Key]
	debug(dInfo, "K%v complete GetRequest %v: %v index: %v", kv.me, args.Key, reply.Value, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// debug(dInfo, "K%v PutAppendLock", kv.me)
	defer func() {
		kv.mu.Unlock()
		// debug(dInfo, "K%v PutAppendUnlock", kv.me)
	}()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	if args.ReqNo <= kv.CompletedPool[args.ClientId] {
		reply.Success = true
		debug(dInfo, "K%v receive duplicated request from C%v", kv.me, args.ClientId)
		return
	}
	debug(dInfo, "K%v receive %vRequest [%v, %v], from C%v", kv.me, args.Op, args.Key, args.Value, args.ClientId)
	op := Op{
		IsGetOp:      false,
		PutAppendArg: *args,
		ClientId:     args.ClientId,
		ReqNo:        args.ReqNo,
	}
	index, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		return
	}
	kv.CompletedCondPool[args.ClientId] = sync.NewCond(&kv.mu)
	for {
		kv.CompletedCondPool[args.ClientId].Wait()
		curTerm, isLeader := kv.rf.GetState()
		if curTerm != startTerm || !isLeader {
			reply.Success = false
			return
		}
		if completedReq, exist := kv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
	}
	reply.Success = true
	debug(dInfo, "K%v complete %vRequest [%v, %v] Index: %v", kv.me, args.Op, args.Key, args.Value, index)
}

func (kv *KVServer) BroadcastAllPeoridly() {
	for !kv.killed() {
		kv.mu.Lock()
		for _, v := range kv.CompletedCondPool {
			v.Broadcast()
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KVMap = make(map[string]string)
	kv.CompletedPool = make(map[int64]int64)
	kv.CompletedCondPool = make(map[int64]*sync.Cond)
	kv.CommitedId = 0
	// kv.SnapshotCond = sync.NewCond(&kv.mu)
	kv.ReadSnapshot(persister.ReadSnapshot())
	go kv.applyLog()
	go kv.BroadcastAllPeoridly()
	go kv.Snapshot()
	return kv
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CommitedId int
	var KVMap map[string]string
	var CompletedPool map[int64]int64
	if d.Decode(&CommitedId) != nil || d.Decode(&CompletedPool) != nil || d.Decode(&KVMap) != nil {
		debug(dError, "K%v Error when decode in ReadSnapshot", kv.me)
		return
	} else {
		kv.CommitedId = CommitedId
		kv.KVMap = KVMap
		kv.CompletedPool = CompletedPool
	}
	kv.rf.Snapshot(kv.CommitedId, data)
	debug(dInfo, "K%v ReadSnapshot Index: %v", kv.me, CommitedId)
}

func (kv *KVServer) Snapshot() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate == -1 || kv.rf.GetRaftStateSize() < kv.maxraftstate {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.CommitedId)
		e.Encode(kv.CompletedPool)
		e.Encode(kv.KVMap)
		data := w.Bytes()
		kv.rf.Snapshot(kv.CommitedId, data)
		debug(dInfo, "K%v Snapshot Index %v, kv.rf.RaftStateSize: %v", kv.me, kv.CommitedId, kv.rf.GetRaftStateSize())
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) applyLog() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			msg := applyMsg.Command.(Op)
			debug(dInfo, "K%v receive clientId: %v, reqNo: %v, index: %v", kv.me, msg.ClientId, msg.ReqNo, applyMsg.CommandIndex)
			if msg.ReqNo > kv.CompletedPool[msg.ClientId] {
				if !msg.IsGetOp {
					switch msg.PutAppendArg.Op {
					case T_PUT:
						kv.KVMap[msg.PutAppendArg.Key] = msg.PutAppendArg.Value
					case T_APPEND:
						kv.KVMap[msg.PutAppendArg.Key] += msg.PutAppendArg.Value
					}
				}
				debug(dInfo, "K%v apply Index %v, clientId: %v, msgReqNo: %v, completedReqNo: %v", kv.me, applyMsg.CommandIndex, msg.ClientId, msg.ReqNo, kv.CompletedPool[msg.ClientId])
				kv.CompletedPool[msg.ClientId] = msg.ReqNo
			}
			if _, exist := kv.CompletedCondPool[msg.ClientId]; exist {
				kv.CompletedCondPool[msg.ClientId].Broadcast()
			}
			kv.CommitedId = max(kv.CommitedId, applyMsg.CommandIndex)
		} else if applyMsg.SnapshotIndex > kv.CommitedId {
			kv.ReadSnapshot(applyMsg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
