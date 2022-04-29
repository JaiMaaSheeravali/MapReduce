package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type TaskType int8

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	EXIT
)

type TaskStatus int8

const (
	UNASSIGNED TaskStatus = iota
	ASSIGNED
	FINISHED
)

// MapReduceTask is exchanged between coordinator and worker
// so that worker knows what action to perform after rpc.
// ---- For a Map task, it’s expected to have exactly 1 filename in InputFiles (in MRT of RequestTaskReply)
// 		and nReduce of filenames in OutputFiles (in MRT of SubmitTaskArgs).
// ---- For a Reduce task, it’s expected to have nReduce of filenames in InputFiles (in MRT of RequestTaskReply)
//		and exactly one filename in OutputFiles (in MRT of SubmitTaskArgs).
type MapReduceTask struct {
	// 0: Map, 1: Reduce, 2: Exit, 3: Wait
	Type TaskType
	// 0: Unassigned, 1: Assigned, 2: Finished
	Status TaskStatus
	// Start Time
	TimeStamp time.Time

	// Index in the list of tasks
	Index int

	InputFiles  []string
	OutputFiles []string
}

// RequestTaskArgs : When a worker requests a task from coordinator,
// worker needs to specify its ip address on which it is listening.
// worker is also acting as RPC server because coordinator needs to
// check if the worker has crashed or not
type RequestTaskArgs struct {
	WorkerAddr string
}

// RequestTaskReply :  The coordinator replies with a MapReduceTask
// which could either be a map job or a reduced job.
// note that the worker (mapper) needs total count of reducer(NReduce)
// to run hash function for partitioning
type RequestTaskReply struct {
	NReduce int
	Task    MapReduceTask
}

// SubmitTaskArgs :  After completing the MapReduceTask, the worker
// sends the details of what has been completed.
// e.g. 1. in case of Map job, the worker would send the list of file locations
// of intermediate key-value pairs, so that in future a reducer could combine them
// 2. in case of Reduce job, the worker sends the output file location
// of key-value pairs
type SubmitTaskArgs struct {
	Task MapReduceTask
}

// SubmitTaskReply Here Master is always available
// Coordinator replies with a success confirmation
type SubmitTaskReply struct{}

// Coordinator polls after every 10 seconds to worker,
// for checking if it is alive and working.
// If no reply comes back from the worker, the coordinator assumes the worker is dead,
// and reassigns the MapReduceTask to another worker
type RequestHealthArgs struct{}
type RequestHealthReply struct{}

// send an RPC request to the rpc server, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(addr string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("dialing: ", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Fatal(err)
	return false
}
