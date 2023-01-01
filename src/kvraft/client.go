package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	me           int64
	recentLeader int
	reqNo        int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var seq int64 = -1

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.recentLeader = 0
	ck.me = atomic.AddInt64(&seq, 1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:      key,
		ClientId: ck.me,
		ReqNo:    atomic.AddInt64(&ck.reqNo, 1),
	}
	reply := GetReply{}
	// keep trying forever, like comments say
	// debug(dInfo, "C%v GetRequest %v", ck.me, key)
	for {
		ok := ck.servers[ck.recentLeader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Success {
			// debug(dInfo, "C%v successfully Get Key: %v, Value: %v", ck.me, key, reply.Value)
			return reply.Value
		}
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && reply.Success {
				ck.recentLeader = i
				// debug(dInfo, "C%v successfully Get Key: %v, Value: %v", ck.me, key, reply.Value)
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		ReqNo:    atomic.AddInt64(&ck.reqNo, 1),
	}
	reply := PutAppendReply{}
	// debug(dInfo, "C%v %vRequest, Key: %v, Value: %v", ck.me, op, key, value)
	for {
		ok := ck.servers[ck.recentLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Success {
			return
		}
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Success {
				ck.recentLeader = i
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
