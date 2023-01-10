package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ShardId  int
	ClientId int64
	ReqNo    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ShardId  int
	ClientId int64
	ReqNo    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardArgs struct {
	ConfigNum int
	Gid       int
	ServerId  int
}

type MigrateShardReply struct {
	Success bool
	KVMap   map[string]string
}

type HelloArgs struct {
	Gid      int
	ServerId int
}
type HelloReply struct {
	ConfigNum int
	Success   bool
}
