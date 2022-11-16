package mr

import (
	"fmt"
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
	nReduce int
	files   map[string]int
	mTasks  map[string]time.Time
	rTasks  map[string]time.Time
	map_mutex sync.Mutex
	reduce_mutex sync.Mutex
}


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MapRpc(args *MapArgs, reply *MapReply) error {

	c.map_mutex.Lock()
	defer c.map_mutex.Unlock()
	fmt.Println("here")
	if c.Done() {
		// coodinator completed !
		reply.Type = -1
		return nil
	}
	// type of maprpc call, 0 repsents ask for a task, 1 repsents finishment of a task
	if args.Type == 0 {
		for k, v := range c.mTasks {
			if time.Since(v) > 10*time.Second {
				reply.Type = 0
				reply.Index = c.files[k]
				reply.FileName = k
				c.mTasks[k] = time.Now()
				return nil
			}
		}

	} else if args.Type == 1 {
		// task complete call, delete task
		if _, exist := c.mTasks[args.FileName]; exist {
			delete(c.mTasks, args.FileName)
			c.rTasks[args.FileName] = time.Now()
		}
	}
	return nil
}

func (c *Coordinator) ReduceRpc(args *ReduceArgs, reply *ReduceReply) error {
	c.reduce_mutex.Lock()
	defer c.reduce_mutex.Unlock()
	if c.Done() {
		// coordinator completed
		reply.Type = -1
		return nil
	}
	if args.Type == 0 {
		for k, v := range c.rTasks {
			if time.Since(v) > 10*time.Second {
				reply.Type = 0
				reply.Index = c.files[k]
				reply.FileName = k
				c.rTasks[k] = time.Now()
				return nil
			}
		}
	} else if args.Type == 1 {
		if _, exist := c.rTasks[args.FileName]; exist {
			delete(c.rTasks, args.FileName)
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
	c.map_mutex.Lock()
	c.reduce_mutex.Lock()
	defer c.map_mutex.Unlock()
	defer c.reduce_mutex.Unlock()
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
	c.nReduce = nReduce
	c.files = make(map[string]int)
	c.mTasks = make(map[string]time.Time)
	c.rTasks = make(map[string]time.Time)
	// t := time.Now()
	for index, v := range files {
		c.files[v] = index
		c.mTasks[v] = time.Now().Add(-1 * time.Hour)
	}
	c.server()
	return &c
}
