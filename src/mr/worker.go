package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// Worker : the RPC argument and reply types are defined in rpc.go.
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// declare an argument structure.
		args := RequestTaskArgs{}

		// declare a reply structure.
		reply := RequestTaskReply{}

		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			break
		}

		switch reply.Task.Type {
		case MAP:
			doMap(&reply, mapf)
		case REDUCE:
			doReduce(&reply, reducef)
		case WAIT:
			fmt.Println("Waiting...")
			time.Sleep(10 * time.Second)
		case EXIT:
			fmt.Println("Successfully completed")
			os.Exit(0)
		}
	}
}

// doMap: For Map task, it’s expected to have 1 filename in InputFiles and nReduce of filenames in OutputFile.
func doMap(r *RequestTaskReply, mapf func(string, string) []KeyValue) {
	numReducers := r.NReduce         // required for partitioning hash function
	filename := r.Task.InputFiles[0] // only one file will be in InputFiles for a map job

	fmt.Fprintf(os.Stdout, "Mapper %d: executing on %s\n", r.Task.Index, filename)

	// send output filenames to coordinator after completion
	// declare an argument structure.
	args := SubmitTaskArgs{}
	args.Task = MapReduceTask{
		Type:       MAP,
		Index:      r.Task.Index,
		InputFiles: r.Task.InputFiles,
		Status:     r.Task.Status,
	}

	// open the input file and read all of its content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		return
	}

	// call the mapf on the content, and partition
	// the key-value array into numReducers partition
	kva := mapf(filename, string(content))
	partitionedKva := make([][]KeyValue, numReducers)
	for _, kv := range kva {
		hashId := ihash(kv.Key) % numReducers
		partitionedKva[hashId] = append(partitionedKva[hashId], kv)
	}

	// Mapper writes down the partitioned key value pairs
	// 		to file system in json format for reducer to read.
	// Note: in real mapreduce, mapper would write to the local disk
	// 		and reducer would perform rpc to mapper to get the key-value pairs directly
	for _, kva := range partitionedKva {
		// convert the key-value array to JSON string
		jsonKva, err := json.Marshal(kva)
		if err != nil {
			log.Fatalf("JSON marshaling failed: %s", err)
		}

		// worker sometimes fails when processing the task. It might happen to write result to output files halfway.
		// To avoid those garbage outputs, worker should be designed to write to an temp file and only when the
		// entire task gets submitted, master then marks them are valid output.
		// note: tmpfile gets deleted automatically if the program exits
		tmpfile, err := ioutil.TempFile("mr-tmp/", "mr")
		if err != nil {
			log.Fatal(err)
		}

		if _, err := tmpfile.Write(jsonKva); err != nil {
			log.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		// mr-x-i.json, where x is the mapper id, i: the partition number
		args.Task.OutputFiles = append(args.Task.OutputFiles, tmpfile.Name())
	}
	args.Task.TimeStamp = time.Now()

	// declare a reply structure.
	reply := SubmitTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.SubmitTask", &args, &reply)
	if !ok {
		log.Fatal("rpc failed")
	}

	fmt.Fprintf(os.Stdout, "Mapper %d: completed\n", r.Task.Index)

}

// doReduce: For Reduce task, it’s expected to have nReduce of filenames in InputFiles and 1 file name in OutputFile
func doReduce(r *RequestTaskReply, reducef func(string, []string) string) {
	// send output filename to coordinator after completion
	// declare an argument structure.
	fmt.Fprintf(os.Stdout, "Reducer %d: started executing\n", r.Task.Index)

	args := SubmitTaskArgs{}
	args.Task = MapReduceTask{
		Type:       REDUCE,
		Index:      r.Task.Index,
		InputFiles: r.Task.InputFiles,
		Status:     r.Task.Status,
	}

	var kva []KeyValue
	for _, filename := range r.Task.InputFiles {
		// open the intermediate file and read all of its content
		intermediateFile, err := os.Open("mr-tmp/" + filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		jsonKva, err := ioutil.ReadAll(intermediateFile)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		err = intermediateFile.Close()
		if err != nil {
			return
		}

		var tempKva []KeyValue
		if err := json.Unmarshal(jsonKva, &tempKva); err != nil {
			log.Fatalf("JSON unmarshaling failed: %s", err)
		}

		kva = append(kva, tempKva...)
	}

	fmt.Fprintf(os.Stdout, "Reducer %d: started sorting\n", r.Task.Index)

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", r.Task.Index)
	ofile, _ := os.Create(oname)

	fmt.Fprintf(os.Stdout, "Reducer %d: completed sorting\n", r.Task.Index)

	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-x, where x is the reducer id.
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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err := ofile.Close()
	if err != nil {
		log.Fatal("Error closing output file ", oname, err)
	}

	args.Task.OutputFiles = []string{oname}
	args.Task.TimeStamp = time.Now()

	// declare a reply structure.
	reply := SubmitTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.SubmitTask", &args, &reply)
	if !ok {
		log.Fatal("rpc failed")
	}

	fmt.Fprintf(os.Stdout, "Reducer %d: completed\n", r.Task.Index)

}

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
