package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nfile   int
	nReduce int
	files   map[string]int
	mTasks  map[string]time.Time
	rTasks  map[int]time.Time
	mut     sync.Mutex
}

func (c *Coordinator) GetCoordinatorStatus() CoordinatorStatus {
	if len(c.mTasks) == 0 && len(c.rTasks) == 0 {
		return ShutDowned
	} else if len(c.mTasks) != 0 {
		return MapStatus
	} else if len(c.rTasks) != 0 {
		return ReduceStatus
	} else {
		return InitStatus
	}
}

func (c *Coordinator) DispatchTask(args *Args, reply *Reply) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	status := c.GetCoordinatorStatus()
	reply.MasterStatus = status
	reply.ReplyCode = NoTask
	reply.NFile = c.nfile
	reply.NReduce = c.nReduce
	if status == ShutDowned {
		return nil
	}
	switch args.ReqType {
	case MapRequest:
		if status == MapStatus {
			for fileName, lastTime := range c.mTasks {
				if time.Since(lastTime) > 10*time.Second {
					reply.ReplyCode = NewTask
					reply.MapFileName = fileName
					reply.MapTaskId = c.files[fileName]
					break
				}
			}
			if reply.ReplyCode == NewTask {
				c.mTasks[reply.MapFileName] = time.Now()
			}
		}
	case ReduceRequest:
		if status == ReduceStatus {
			for taskId, lastTime := range c.rTasks {
				if time.Since(lastTime) > 10*time.Second {
					reply.ReplyCode = NewTask
					reply.ReduceTaskId = taskId
					break
				}
			}
			if reply.ReplyCode == NewTask {
				c.rTasks[reply.ReduceTaskId] = time.Now()
			}
		}
	case MapFinishRequest:
		if status == MapStatus {
			_, exists := c.mTasks[args.MapFinishedFileName]
			if exists {
				delete(c.mTasks, args.MapFinishedFileName)
				if len(c.mTasks) == 0 {
					for i := 0; i < c.nReduce; i++ {
						c.rTasks[i] = time.Now().Add(-1 * time.Hour)
					}
				}
			}
		}
	case ReduceFinishRequest:
		if status == ReduceStatus {
			_, exists := c.rTasks[args.ReduceFinishedTaskId]
			if exists {
				delete(c.rTasks, args.ReduceFinishedTaskId)
			}
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mut.Lock()
	defer c.mut.Unlock()
	if len(c.mTasks) == 0 && len(c.rTasks) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nfile = len(files)
	c.nReduce = nReduce
	c.files = make(map[string]int)
	c.mTasks = make(map[string]time.Time)
	c.rTasks = make(map[int]time.Time)
	for index, v := range files {
		c.files[v] = index
		c.mTasks[v] = time.Now().Add(-1 * time.Hour)
	}
	c.server()
	return &c
}
