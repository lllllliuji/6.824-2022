package kvraft

import (
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
	// Tag          int64
	ClientId int64
	ReqNo    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVMap         map[string]string
	CompletedPool map[int64]int64
	// CompletedCond *sync.Cond
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
	_, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		return
	}
	for {
		// debug(dInfo, "GetHere")
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != startTerm {
			reply.Success = false
			return
		}
		if completedReq, exist := kv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
		// kv.CompletedCond.Wait()
		kv.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
		kv.mu.Lock()
	}
	reply.Value = kv.KVMap[args.Key]
	reply.Success = true
	debug(dInfo, "K%v complete GetRequest %v: %v", kv.me, args.Key, reply.Value)
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
	_, startTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		return
	}
	for {
		// debug(dInfo, "PutAppendHere")
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != startTerm {
			reply.Success = false
			return
		}
		if completedReq, exist := kv.CompletedPool[args.ClientId]; exist && completedReq == args.ReqNo {
			break
		}
		// kv.CompletedCond.Wait()
		kv.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
		kv.mu.Lock()
	}
	reply.Success = true
	debug(dInfo, "K%v complete %vRequest [%v, %v]", kv.me, args.Op, args.Key, args.Value)
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
	// kv.CompletedCond = sync.NewCond(&kv.mu)
	go kv.applyLog()
	return kv
}

func (kv *KVServer) applyLog() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		// debug(dInfo, "ApplyLock")
		msg := applyMsg.Command.(Op)
		if msg.ReqNo > kv.CompletedPool[msg.ClientId] {
			if !msg.IsGetOp {
				switch msg.PutAppendArg.Op {
				case T_PUT:
					kv.KVMap[msg.PutAppendArg.Key] = msg.PutAppendArg.Value
				case T_APPEND:
					kv.KVMap[msg.PutAppendArg.Key] += msg.PutAppendArg.Value
				}
			}
			kv.CompletedPool[msg.ClientId] = msg.ReqNo
		}
		// kv.CompletedCond.Broadcast()
		kv.mu.Unlock()
	}
}
