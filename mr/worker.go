package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const WaitTimeDuration = time.Second / 5

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskInfo{}
		reply := TaskInfo{}

		// send the RPC request, wait for the reply.
		call("Master.AssignWork", &args, &reply)
		switch reply.TaskKind {
		case TaskKindMap:
			println("Begin map task, filename: ", reply.FileName)
			filenameToMap := reply.FileName
			fileToMap, err := os.Open(filenameToMap)
			if err != nil {
				log.Fatalf("cannot open %v", filenameToMap)
			}
			content, err := ioutil.ReadAll(fileToMap)
			if err != nil {
				log.Fatalf("cannot read %v", filenameToMap)
			}
			fileToMap.Close()
			kva := mapf(filenameToMap, string(content))
			kvaListToReduce := make([][]KeyValue, reply.ReduceTasksSum)
			for _, kv := range kva {
				order := ihash(kv.Key)%reply.ReduceTasksSum
				kvaListToReduce[order] = append(kvaListToReduce[order], kv)
			}
			for index, kvaToReduce := range kvaListToReduce {
				filenameToReduce := "mr-" + strconv.Itoa(reply.TaskOrder) + "-" + strconv.Itoa(index)
				fileToReduce, err := os.Create(filenameToReduce)
				if err != nil {
					log.Fatalf("cannot create %v", filenameToReduce)
				}
				enc := json.NewEncoder(fileToReduce)
				for _, kv := range kvaToReduce{
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Encode error, key:%v, value:%v", kv.Key, kv.Value)
					}
				}

				fileToReduce.Close()
			}
			println("Map task done, filename: ", reply.FileName)
			call("Master.WorkDone", &reply, &args)
		case TaskKindReduce:
			println("Begin reduce task, order: ", reply.TaskOrder)
			var kva []KeyValue
			for i := 0; i < reply.MapTasksSum; i++ {
				filenameToReduce := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskOrder)
				fileToReduce, err := os.Open(filenameToReduce)
				if err != nil {
					log.Fatalf("cannot open %v", filenameToReduce)
				}
				dec := json.NewDecoder(fileToReduce)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				fileToReduce.Close()
			}

			sort.Sort(ByKey(kva))

			oName := "mr-out-" + strconv.Itoa(reply.TaskOrder)
			oFile, _ := os.Create(oName)
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-X.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			oFile.Close()
			println("Reduce task done, order: ", reply.TaskOrder)
			call("Master.WorkDone", &reply, &args)
		case TaskKindWait:
			time.Sleep(WaitTimeDuration)
		case TaskKindEnd:
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
	fmt.Printf("reply.Z %v\n", reply.Z)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
