package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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
	Id           int64
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
	CompletedPool map[int64]string
	CompletedCond *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	debug(dInfo, "Leader S%v receive Get Request Get %v, from C%v", kv.me, args.Key, args.ClientId)
	op := Op{
		IsGetOp: true,
		GetArg:  *args,
		Id:      nrand(),
	}
	kv.rf.Start(op)
	// debug(dInfo, "Start %v", op.Id)
	for _, exist := kv.CompletedPool[op.Id]; !exist; {
		kv.CompletedCond.Wait()
		_, exist = kv.CompletedPool[op.Id]
	}
	reply.Value = kv.CompletedPool[op.Id]
	delete(kv.CompletedPool, op.Id)
	reply.Success = true
	debug(dInfo, "S%v Complete GetRequest %v", kv.me, args.Key)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Success = false
		return
	}
	debug(dInfo, "S%v receive %vRequest [%v, %v], from C%v", kv.me, args.Op, args.Key, args.Value, args.ClientId)
	op := Op{
		IsGetOp:      false,
		PutAppendArg: *args,
		Id:           nrand(),
	}
	kv.rf.Start(op)
	// debug(dInfo, "Start %v", op.Id)
	for _, exist := kv.CompletedPool[op.Id]; !exist; {
		kv.CompletedCond.Wait()
		_, exist = kv.CompletedPool[op.Id]
	}
	delete(kv.CompletedPool, op.Id)
	reply.Success = true
	debug(dInfo, "S%v Complete %vRequest [%v, %v]", args.Op, args.Key, args.Value)
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
	kv.CompletedCond = sync.NewCond(&kv.mu)
	kv.KVMap = make(map[string]string)
	kv.CompletedPool = make(map[int64]string)
	go kv.applyLog()
	return kv
}

func (kv *KVServer) applyLog() {
	for !kv.killed() {
		for applyMsg := range kv.applyCh {
			kv.mu.Lock()
			var msg Op = applyMsg.Command.(Op)
			if msg.IsGetOp {
				kv.CompletedPool[msg.Id] = kv.KVMap[msg.GetArg.Key]
			} else {
				switch msg.PutAppendArg.Op {
				case T_PUT:
					kv.KVMap[msg.PutAppendArg.Key] = msg.PutAppendArg.Value
				case T_APPEND:
					kv.KVMap[msg.PutAppendArg.Key] += msg.PutAppendArg.Value
				}
				kv.CompletedPool[msg.Id] = ""
			}
			// debug(dInfo, "Commited %v", msg.Id)
			kv.CompletedCond.Broadcast()
			kv.mu.Unlock()
		}
	}
}
