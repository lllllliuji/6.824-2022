package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type CoordinatorStatus int32
type RequestType int32
type ReplyType int32
const (
	InitStatus CoordinatorStatus = 0
	MapStatus CoordinatorStatus = 1
	ReduceStatus CoordinatorStatus = 2
	ShutDowned CoordinatorStatus = 3

	MapRequest RequestType = 1
	ReduceRequest RequestType = 2
	MapFinishRequest RequestType = 3
	ReduceFinishRequest RequestType = 4

	NoTask ReplyType = 0
	NewTask ReplyType = 1
)
type Args struct {
	ReqType RequestType
	MapFinishedFileName string
	ReduceFinishedTaskId int
}

type Reply struct {
	NFile int
	NReduce int
	MasterStatus CoordinatorStatus
	MapTaskId int
	MapFileName string
	ReduceTaskId int
	ReplyCode ReplyType
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapArgs struct {
	Type int
	FileName string
}

type MapReply struct {
	Type int
	Index int
	FileName string
}

type ReduceArgs struct {
	Type int
	FileName string
}

type ReduceReply struct {
	Type int
	Index int
	FileName string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
