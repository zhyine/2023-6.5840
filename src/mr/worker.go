package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := os.Getegid()
	preTaskId := -1
	preTaskType := ""
	log.Printf("Prepare to start Worker %d, and request tasks from Coordinator", workerId)
	// Your worker implementation here.
	// Keep requesting Task
	for {
		args := RequestTaskArgs{
			WorkerId:    workerId,
			PreTaskId:   preTaskId,
			PreTaskType: preTaskType,
		}
		reply := RequestTaskReply{}
		call("Coordinator.RequestTask", &args, &reply)
		switch reply.TaskType {
		case "":
			log.Printf("All tasks have finished.\n")
			goto BREAK
		case MAP:
			performMapTask(workerId, reply.TaskId, reply.MapInputFile, reply.NReduce, mapf)
		case REDUCE:
			performReduceTask(workerId, reply.TaskId, reply.NMap, reducef)
		}
		preTaskId = reply.TaskId
		preTaskType = reply.TaskType
		log.Printf("%s Task %d has finished.\n", reply.TaskType, reply.TaskId)
	}
BREAK:
	log.Printf("Worker %d end its job.\n", workerId)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func performMapTask(workerId int, taskId int, inputFile string, nReduce int, mapf func(string, string) []KeyValue) {
	log.Printf("worker(%d) start to perform map task(%d)", workerId, taskId)
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v.\n", inputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v.\n", inputFile)
	}
	file.Close()
	kva := mapf(inputFile, string(content))

	hashKva := map[int][]KeyValue{}
	for _, kv := range kva {
		hashK := ihash(kv.Key) % nReduce
		hashKva[hashK] = append(hashKva[hashK], kv)
	}

	for i := 0; i < nReduce; i++ {
		tempMapOutputFile, _ := os.Create(getTempMapOutput(workerId, taskId, i))
		for _, kv := range hashKva[i] {
			fmt.Fprintf(tempMapOutputFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		tempMapOutputFile.Close()
	}
}

func performReduceTask(workerId int, taskId int, nMap int, reducef func(string, []string) string) {
	log.Printf("worker(%d) start to perform reduce task(%d)", workerId, taskId)
	var allIntermediate []string
	for i := 0; i < nMap; i++ {
		file, err := os.Open(getFinalMapOutput(i, taskId))
		if err != nil {
			log.Fatalf("cannot open %v.\n", getFinalMapOutput(i, taskId))
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v.\n", getFinalMapOutput(i, taskId))
		}
		allIntermediate = append(allIntermediate, strings.Split(string(content), "\n")...)
	}

	var kva []KeyValue
	for _, intermediate := range allIntermediate {
		if strings.TrimSpace(intermediate) == "" {
			continue
		}
		kv := strings.Split(intermediate, "\t")
		kva = append(kva, KeyValue{
			Key:   kv[0],
			Value: kv[1],
		})
	}

	sort.Sort(ByKey(kva))
	tempReduceOutputFile, _ := os.Create(getTempReduceOutput(workerId, taskId))

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
		fmt.Fprintf(tempReduceOutputFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempReduceOutputFile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func getTempMapOutput(workId int, mapId int, reduceId int) string {
	return fmt.Sprintf("temp-worker(%d)-map(%d)-reduce(%d)", workId, mapId, reduceId)
}

func getTempReduceOutput(workId int, reduceId int) string {
	return fmt.Sprintf("temp-worker(%d)-reduce(%d)", workId, reduceId)
}

func getFinalMapOutput(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func getFinalReduceOutput(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
