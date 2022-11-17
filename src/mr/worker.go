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

func MapTaskWoker(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := Args{}
		reply := Reply{}
		args.ReqType = MapRequest
		ok := call("Coordinator.DispatchTask", &args, &reply)
		if !ok {
			log.Fatal("MapTaskWorker Failed")
			time.Sleep(time.Second)
			continue
		}
		if reply.MasterStatus != MapStatus {
			return
		}
		if reply.ReplyCode == NoTask {
			time.Sleep(time.Second)
			continue
		} else if reply.ReplyCode == NewTask {
			file, err := os.Open(reply.MapFileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapFileName)
				time.Sleep(time.Second)
				continue
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapFileName)
				time.Sleep(time.Second)
				continue
			}
			file.Close()
			intermediate := mapf(reply.MapFileName, string(content))
			sort.Sort(ByKey(intermediate))
			kvlists := make([][]KeyValue, reply.NReduce)
			for _, kv := range intermediate {
				kvlists[ihash(kv.Key) % reply.NReduce] = append(kvlists[ihash(kv.Key) % reply.NReduce], kv)
			}
			for index, kvlist := range kvlists {
				// ofile, _ := os.Create(onname)
				// enc := json.NewEncoder(ofile)
				// for _, kv := range kvlist {
				// 	enc.Encode(&kv)
				// }
				// ofile.Close()
				tmpfile, _ := ioutil.TempFile("", "mr-x-y-tmp")
				enc := json.NewEncoder(tmpfile)
				for _, kv := range kvlist {
					enc.Encode(&kv)
				}
				tmpfile.Close()
				onname := "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(index)
				os.Rename(tmpfile.Name(), "./" + onname)
			}
			finish_arg := Args{}
			finishi_reply := Reply{}
			finish_arg.ReqType = MapFinishRequest
			finish_arg.MapFinishedFileName = reply.MapFileName
			call("Coordinator.DispatchTask", &finish_arg, &finishi_reply)
		}
	}
}
func ReduceTaskWorker(reducef func(string, []string) string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		args := Args{}
		reply := Reply{}
		args.ReqType = ReduceRequest
		ok := call("Coordinator.DispatchTask", &args, &reply)
		if !ok {
			log.Fatal("ReduceTaskWorker Request Failed")
			time.Sleep(time.Second)
			continue
		}
		if reply.MasterStatus == ShutDowned {
			return
		} else if reply.MasterStatus == ReduceStatus {
			if reply.ReplyCode == NewTask {
				result := make(map[string][]string)
				Y := reply.ReduceTaskId
				for X := 0; X < reply.NFile; X++ {
					oname := "mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(Y)
					ofile, err := os.Open(oname)
					if err != nil {
						log.Fatalf("connot open %v", reply.MapFileName)
						time.Sleep(time.Second)
						continue
					}
					dec := json.NewDecoder(ofile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						// value, err := strconv.Atoi(kv.Value)
						// if err != nil {
						// 	log.Fatal("Invalid KeyValue")
						// }
						// result[kv.Key] += value
						result[kv.Key] = append(result[kv.Key], kv.Value)
					}
					ofile.Close()
				}
				resultFileName := "mr-out-" + strconv.Itoa(Y)
				// rfile, _ := os.Create(resultFileName)
				// for k, v := range result {
				// 	fmt.Fprintf(rfile, "%v %v\n", k, v)
				// }
				// rfile.Close()
				tmpfile, _ := ioutil.TempFile("", "mr-out-tmp")
				for k, v := range result {
					fmt.Fprintf(tmpfile, "%v %v\n", k, reducef(k, v))
				}
				tmpfile.Close()
				os.Rename(tmpfile.Name(), "./" + resultFileName)		
				reduce_finish_req := Args{}
				reduce_finish_reply := Reply{}
				reduce_finish_req.ReqType = ReduceFinishRequest
				reduce_finish_req.ReduceFinishedTaskId = reply.ReduceTaskId
				call("Coordinator.DispatchTask", &reduce_finish_req, &reduce_finish_reply)
			} else {
				time.Sleep(time.Second)
				continue
			}
		}
	}
}

// func MapWorker(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	for {
// 		args := MapArgs{}
// 		reply := MapReply{}
// 		args.Type = 0
// 		ok := call("Coordinator.MapRpc", &args, &reply)
// 		if !ok {
// 			log.Fatal("MapRpc failed")
// 			continue
// 		}
// 		if reply.Type == 0 {

// 			file, err := os.Open(reply.FileName)
// 			if err != nil {
// 				log.Fatalf("cannot open %v", reply.FileName)
// 			}
// 			content, err := ioutil.ReadAll(file)
// 			if err != nil {
// 				log.Fatalf("cannot read %v", reply.FileName)
// 			}
// 			file.Close()
// 			intermediate := mapf(reply.FileName, string(content))
// 			// write to intermediate file
// 			oname := "intermediate-" + strconv.Itoa(reply.Index)
// 			ofile, _ := os.Create(oname)
// 			enc := json.NewEncoder(file)
// 			for _, kv := range intermediate {
// 				enc.Encode(&kv)
// 			}
// 			ofile.Close()
// 			finish_map_args := MapArgs{}
// 			finish_map_reply := MapReply{}
// 			finish_map_args.Type = 1
// 			finish_map_args.FileName = oname
// 			call("Coordinator.MapRpc", &finish_map_args, &finish_map_reply)
// 		} else if reply.Type == 1 {
// 			// busy
// 			time.Sleep(time.Second)
// 		} else {
// 			break
// 		}
// 	}
// }

// func ReduceWorker(reducef func(string, []string) string, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	for {
// 		args := ReduceArgs{}
// 		reply := ReduceReply{}
// 		args.Type = 0
// 		ok := call("Coordinator.ReduceRpc", &args, &reply)
// 		if !ok {
// 			log.Fatal("ReduceRpc failed")
// 			continue
// 		}
// 		if reply.Type == 0 {
// 			file, err := os.Open(reply.FileName)
// 			if err != nil {
// 				log.Fatalf("cannot open %v", reply.FileName)
// 			}
// 			dec := json.NewDecoder(file)
// 			var kva []KeyValue
// 			for {
// 				var kv KeyValue
// 				if err := dec.Decode(&kv); err != nil {
// 					break
// 				}
// 				kva = append(kva, kv)
// 			}
// 			file.Close()
// 			// reduce part
// 			sort.Sort(ByKey(kva))
// 			oname := "mr-out-" + strconv.Itoa(reply.Index)
// 			ofile, _ := os.Create(oname)
// 			i := 0
// 			for i < len(kva) {
// 				j := i + 1
// 				for j < len(kva) && kva[j].Key == kva[i].Key {
// 					j++
// 				}
// 				values := []string{}
// 				for k := i; k < j; k++ {
// 					values = append(values, kva[k].Value)
// 				}
// 				output := reducef(kva[i].Key, values)
// 				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
// 				i = j
// 			}
// 			ofile.Close()
// 			finish_reduce_args := ReduceArgs{}
// 			finish_reduce_reply := ReduceReply{}
// 			finish_reduce_args.Type = 1
// 			finish_reduce_args.FileName = oname
// 			call("Coordinator.ReduceRpc", &finish_reduce_args, &finish_reduce_reply)
// 		} else if reply.Type == 1 {
// 			// busy
// 			time.Sleep(time.Second)
// 		} else {
// 			break
// 		}
// 	}

// }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// var wg sync.WaitGroup
	// wg.Add(2)
	// go MapWorker(mapf, &wg)
	// go ReduceWorker(reducef, &wg)
	// wg.Wait()
	var wg sync.WaitGroup
	wg.Add(2)
	go MapTaskWoker(mapf, &wg)
	go ReduceTaskWorker(reducef, &wg)
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
