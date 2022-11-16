package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapWorker(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := MapArgs{}
		reply := MapReply{}
		args.Type = 0
		ok := call("Coordinator.MapRpc", &args, &reply)
		if !ok {
			log.Fatal("MapRpc failed")
			continue
		}
		if reply.Type == 0 {
			
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			intermediate := mapf(reply.FileName, string(content))
			// write to intermediate file
			oname := "intermediate-" + strconv.Itoa(reply.Index)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(file)
			for _, kv := range intermediate {
				enc.Encode(&kv)
			}
			ofile.Close()
			finish_map_args := MapArgs{}
			finish_map_reply := MapReply{}
			finish_map_args.Type = 1
			finish_map_args.FileName = oname
			call("Coordinator.MapRpc", &finish_map_args, &finish_map_reply)
		} else if reply.Type == 1 {
			// busy
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

func ReduceWorker(reducef func(string, []string) string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := ReduceArgs{}
		reply := ReduceReply{}
		args.Type = 0
		ok := call("Coordinator.ReduceRpc", &args, &reply)
		if !ok {
			log.Fatal("ReduceRpc failed")
			continue
		}
		if reply.Type == 0 {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			dec := json.NewDecoder(file)
			var kva []KeyValue
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
			// reduce part
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(reply.Index)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
			finish_reduce_args := ReduceArgs{}
			finish_reduce_reply := ReduceReply{}
			finish_reduce_args.Type = 1
			finish_reduce_args.FileName = oname
			call("Coordinator.ReduceRpc", &finish_reduce_args, &finish_reduce_reply)
		} else if reply.Type == 1 {
			// busy
			time.Sleep(time.Second)
		} else {
			break
		}
	}

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var wg sync.WaitGroup
	wg.Add(2)
	go MapWorker(mapf, &wg)
	go ReduceWorker(reducef, &wg)
	wg.Wait()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
