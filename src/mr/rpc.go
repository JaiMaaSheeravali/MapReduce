package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

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
// it does not need to specify anything.
type RequestTaskArgs struct{}

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
