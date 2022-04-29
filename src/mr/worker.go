package mr

import (
	"encoding/json"
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"github.com/joho/godotenv"
	"io/ioutil"
	"net"
	"net/http"
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

func goDotEnvVariable(key string) string {

	// load .env file
	err := godotenv.Load("../.env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	return os.Getenv(key)
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

// craete a new file, overwrite if already exist
func createHdfsFile(hdfsClient *hdfs.Client, filename string) (*hdfs.FileWriter, error) {
	/* Removing file is important as
	if the worker crash in between then the
	intermeditate file needs to be overwritten
	*/

	hdfsClient.Remove(filename)

	handle, err := hdfsClient.Create(filename)
	if err != nil {
		fmt.Println("failed to create file ", filename)
	}
	return handle, err
}

type Worker struct{}

func (w *Worker) server() net.Addr {
	err := rpc.Register(w)
	if err != nil {
		log.Fatal(w, "rpc could not be registered: ", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":0")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	return l.Addr()
}

func (w *Worker) HealthStatus(args *RequestHealthArgs, reply *RequestHealthReply) error {
	return nil
}

// MakeWorker : the RPC argument and reply types are defined in rpc.go.
// main/mrworker.go calls this function.
func MakeWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, coordinatorPort string) {

	// connect to the hadoop file system
	hdfsClient, err := hdfs.New("localhost:9000")

	if err != nil {
		log.Fatal("Couldn't connect to HDFS\n", err)
	}

	// create a rpc server on Worker for coordinator to poll the health status of a MapReduceTask
	w := Worker{}
	workerAddr := w.server()

	for {
		// declare an argument structure.
		args := RequestTaskArgs{workerAddr.String()}

		// declare a reply structure.
		reply := RequestTaskReply{}

		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("127.0.0.1:"+coordinatorPort, "Coordinator.RequestTask", &args, &reply)
		if !ok {
			break
		}

		switch reply.Task.Type {
		case MAP:
			doMap(hdfsClient, &reply, mapf, coordinatorPort)
		case REDUCE:
			doReduce(hdfsClient, &reply, reducef, coordinatorPort)
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
func doMap(hdfsClient *hdfs.Client, r *RequestTaskReply, mapf func(string, string) []KeyValue, coordinatorPort string) {
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
	//file, err := os.Open(filename)
	file, err := hdfsClient.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s\n%v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Fatal("cannot close input file ", filename, err)
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
	var tmpFiles []string
	for _, kva := range partitionedKva {
		// convert the key-value array to JSON string
		jsonKva, err := json.Marshal(kva)
		if err != nil {
			log.Fatalf("JSON marshaling failed: %s", err)
		}

		// worker sometimes fails when processing the task. It might happen to write result to output files halfway.
		// To avoid those garbage outputs, worker should be designed to write to a temp file.
		// note: tmpfile will be deleted if the program exits

		tmpfile, err := ioutil.TempFile("mr-tmp/", "mr")
		if err != nil {
			log.Fatal(err)
		}

		if _, err := tmpfile.Write(jsonKva); err != nil {
			log.Fatal(err)
		}
		tmpFiles = append(tmpFiles, tmpfile.Name())
	}

	for i, tmpfile := range tmpFiles {
		// rename the temporary intermediate files created above and write to hdfs
		// validName :=
		// 		mapTask:    mr-<mapTask_idx>-<reduceTask_idx>
		//      here mapTask_idx is "task.Index" and reduceTask_idx is "i"
		//		reduceTask: mr-out-<reduceTask_idx>
		intermediateFilename := fmt.Sprintf(goDotEnvVariable("HDFS_TEMP_PATH")+"mr-%d-%d", r.Task.Index, i)
		args.Task.OutputFiles = append(args.Task.OutputFiles, intermediateFilename)

		// transfer temp file contents to HDFS
		// by creating a new intermediate file in HDFS
		handle, err := createHdfsFile(hdfsClient, intermediateFilename)

		if err != nil {
			log.Fatalf("Mapper %d could not create intermediate file %s %s", r.Task.Index, intermediateFilename, err)
		}

		// copy temp file content to HDFS file
		buf, err := ioutil.ReadFile(tmpfile)
		if err != nil {
			log.Fatal("Reading temp file failed", err)
		}

		len, err := handle.Write(buf)

		fmt.Println("Number of bytes return on intermediate file -> ", len)
		if err != nil {
			log.Fatal("Writing mr- hdfs", err)
		}
		err = handle.Close()
		if err != nil {
			log.Fatal("Closing failed on HDFS", err)
		}
		// delete the temp file
		// tmpfile.Close()
		err = os.Remove(tmpfile)
		if err != nil {
			log.Fatal("Removing temp file failed", err)
		}
	}

	args.Task.TimeStamp = time.Now()

	// declare a reply structure.
	reply := SubmitTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("127.0.0.1:"+coordinatorPort, "Coordinator.SubmitTask", &args, &reply)
	if !ok {
		log.Fatal("rpc failed")
	}

	fmt.Fprintf(os.Stdout, "Mapper %d: completed\n", r.Task.Index)

}

// doReduce: For Reduce task, it’s expected to have nReduce of filenames in InputFiles and 1 file name in OutputFile
func doReduce(hdfsClient *hdfs.Client, r *RequestTaskReply, reducef func(string, []string) string, coordinatorPort string) {
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
		// intermediateFile, err := os.Open("mr-tmp/" + filename)
		intermediateFile, err := hdfsClient.Open(filename)
		if err != nil {
			log.Fatal("cannot open ", filename, err)
		}
		jsonKva, err := ioutil.ReadAll(intermediateFile)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		err = intermediateFile.Close()
		if err != nil {
			log.Fatal("cannot close ", filename, " ", err)
		}

		var tempKva []KeyValue
		if err := json.Unmarshal(jsonKva, &tempKva); err != nil {
			log.Fatalf("JSON unmarshaling failed: %s", err)
		}

		kva = append(kva, tempKva...)
	}

	fmt.Fprintf(os.Stdout, "Reducer %d: started sorting\n", r.Task.Index)

	sort.Sort(ByKey(kva))
	fmt.Fprintf(os.Stdout, "Reducer %d: completed sorting\n", r.Task.Index)

	oname := fmt.Sprintf(goDotEnvVariable("HDFS_OUTPUT_PATH")+"mr-out-%d", r.Task.Index)
	// ofile, _ := os.Create(oname)
	ofile, err := createHdfsFile(hdfsClient, oname)
	if err != nil {
		log.Fatalf("Reducer %d couldn't create output file %s %s", r.Task.Index, oname, err)
	}

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

	err = ofile.Close()
	if err != nil {
		log.Fatal("Error closing output file ", oname, err)
	}

	args.Task.OutputFiles = []string{oname}
	args.Task.TimeStamp = time.Now()

	// declare a reply structure.
	reply := SubmitTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("127.0.0.1:"+coordinatorPort, "Coordinator.SubmitTask", &args, &reply)
	if !ok {
		log.Fatal("rpc failed reducer")
	}

	fmt.Fprintf(os.Stdout, "Reducer %d: completed\n", r.Task.Index)
}
