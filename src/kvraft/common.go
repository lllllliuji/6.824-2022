package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	T_PUT    string = "Put"
	T_APPEND string = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	ReqNo    int64
}

type PutAppendReply struct {
	Err     Err
	Success bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	ReqNo    int64
}

type GetReply struct {
	Err     Err
	Value   string
	Success bool
}
