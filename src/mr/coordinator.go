package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	inputFiles []string
	nReduce    int

	mapTasks    []MapReduceTask
	reduceTasks []MapReduceTask

	// Increase by 1 when one mapTask Done. The map Phase is Done when mapDone == inputFiles
	mapDone int
	// Increase by 1 when one reduceTask Done. The reduce Phase is Done when reduceDone == nReduce
	reduceDone int

	// Each time allow only one worker to update
	mutex sync.Mutex
}

// helper function
func taskTypeFromEnum(taskType TaskType) string {
	var ret string
	switch taskType {
	case MAP:
		ret = "MAPPER"
	case REDUCE:
		ret = "REDUCER"
	case WAIT:
		ret = "WAITING"
	case EXIT:
		ret = "EXITING"
	}
	return ret
}

func (c *Coordinator) checkWorkerHealthStatus(workerAddr string, task *MapReduceTask) {
	for {
		time.Sleep(10 * time.Second)
		c.mutex.Lock()

		if task.Status == ASSIGNED {
			ok := call(workerAddr, "Worker.HealthStatus", &RequestHealthArgs{}, &RequestHealthReply{})
			if !ok {
				task.Status = UNASSIGNED
				fmt.Println(taskTypeFromEnum(task.Type), task.Index, ": Timeout Reassigning task...")
				c.mutex.Unlock()
				break
			}
		} else if task.Status == FINISHED {
			c.mutex.Unlock()
			break
		}
		c.mutex.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.NReduce = c.nReduce

	// map phase
	if c.mapDone < len(c.inputFiles) {

		// find the first unassigned maptask, if any
		for i, task := range c.mapTasks {

			if task.Status == UNASSIGNED {
				task.Status = ASSIGNED
				task.TimeStamp = time.Now()

				fmt.Fprintf(os.Stdout, "Mapper %d: working\n", task.Index)
				c.mapTasks[i] = task
				reply.Task = task

				go c.checkWorkerHealthStatus(args.WorkerAddr, &c.mapTasks[i])

				return nil
			}
		}
		// the map phase is yet to be completed, so wait for 10 sec
		fmt.Fprintf(os.Stdout, "Worker Waiting for Map Jobs to complete\n")
		reply.Task.Type = WAIT
	} else if c.reduceDone < c.nReduce { // reduce phase
		// find the first unassigned reducetask, if any
		for i, task := range c.reduceTasks {
			if task.Status == UNASSIGNED {
				task.Status = ASSIGNED
				task.TimeStamp = time.Now()

				fmt.Fprintf(os.Stdout, "Reducer %d: working\n", task.Index)
				c.reduceTasks[i] = task
				reply.Task = task

				go c.checkWorkerHealthStatus(args.WorkerAddr, &c.reduceTasks[i])

				return nil
			}
		}
		// the reduce phase is yet to be completed, so wait for 10 sec.
		// Why not just set the type to exit since this is the last phase?
		// Because it could happen that a reducer node fails,
		// and we have to create a new reducer node
		fmt.Fprintf(os.Stdout, "Worker Waiting for Reduce Jobs to complete\n")
		reply.Task.Type = WAIT
	} else {
		// map and reduce phase both completed
		fmt.Fprintf(os.Stdout, "Worker Exiting Successfully\n")
		reply.Task.Type = EXIT
	}

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := args.Task
	switch task.Type {
	case MAP:
		diff := task.TimeStamp.Sub(c.mapTasks[task.Index].TimeStamp)
		fmt.Fprintf(os.Stdout, "Mapper %d: completed job in %f seconds\n", task.Index, diff.Seconds())

		// update status of the completed maptask
		c.mapTasks[task.Index].Status = FINISHED
		c.mapDone++

		// append the intermediate files generated by the mapper in every reducetask
		for i, intermediateFilename := range task.OutputFiles {
			c.reduceTasks[i].InputFiles = append(c.reduceTasks[i].InputFiles, intermediateFilename)
		}
	case REDUCE:
		diff := task.TimeStamp.Sub(c.reduceTasks[task.Index].TimeStamp)
		fmt.Fprintf(os.Stdout, "Reducer %d: completed job in %f seconds\n", task.Index, diff.Seconds())

		// update status of the completed reducetask
		c.reduceTasks[task.Index].Status = FINISHED
		c.reduceDone++

		// delete all the intermediate files on which the completed reducer worked upon
		// for _, intermediateFilename := range task.InputFiles {
		// 	err := os.Remove("mr-tmp/" + intermediateFilename)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
	default:
		return errors.New("submitted task can only be map or reduce")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal(c, "rpc could not be registered: ", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.reduceDone == c.nReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:  files,
		nReduce:     nReduce,
		mapTasks:    make([]MapReduceTask, len(files)),
		reduceTasks: make([]MapReduceTask, nReduce),
		mapDone:     0,
		reduceDone:  0,
		mutex:       sync.Mutex{},
	}

	// Your code here.
	for i, file := range files {
		newMapTask := MapReduceTask{
			Type:        MAP,
			Index:       i,
			InputFiles:  []string{file},
			OutputFiles: nil,
			Status:      UNASSIGNED,
		}
		c.mapTasks[i] = newMapTask
	}

	for i := 0; i < nReduce; i++ {
		newReduceTask := MapReduceTask{
			Type:        REDUCE,
			Index:       i,
			InputFiles:  nil,
			OutputFiles: nil,
			Status:      UNASSIGNED,
		}
		c.reduceTasks[i] = newReduceTask
	}

	c.server()
	return &c
}
