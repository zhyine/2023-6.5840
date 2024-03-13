package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	END    = "END"
)

type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex

	stage            string
	nMap             int
	nReduce          int
	stateTaskMapping map[string]Task
	unassignedTasks  chan Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := c.stage == END
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:            MAP,
		nMap:             len(files),
		nReduce:          nReduce,
		stateTaskMapping: map[string]Task{},
		unassignedTasks:  make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			Id:           i,
			Type:         MAP,
			MapInputFile: file,
			WorkerId:     -1,
		}
		log.Printf("Initialize task %d, type: %s", task.Id, task.Type)
		c.stateTaskMapping[assignTaskId(task.Type, task.Id)] = task
		c.unassignedTasks <- task
	}
	log.Printf("Coordinator initialization complete, and start the server.")
	c.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.stateTaskMapping {
				if task.WorkerId != -1 && time.Now().After(task.Deadline) {
					log.Printf("Found timed-out %s task %d previously running on worker %v. Prepare to re-assign", task.Type, task.Id, task.WorkerId)
					task.WorkerId = -1
					c.unassignedTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

func assignTaskId(taskType string, taskId int) string {
	return fmt.Sprintf("%s-%d", taskType, taskId)
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if args.PreTaskId != -1 {
		c.lock.Lock()
		preTaskId, preTaskType := args.PreTaskId, args.PreTaskType
		taskId := assignTaskId(preTaskType, preTaskId)
		if task, ok := c.stateTaskMapping[taskId]; ok && task.WorkerId == args.WorkerId {
			if preTaskType == MAP {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(getTempMapOutput(args.WorkerId, preTaskId, i),
						getFinalMapOutput(preTaskId, i))
					if err != nil {
						log.Fatalf("Failed to rename tempMapOutputFile to finalMapOutputFile.\n")
					}
				}
			} else if preTaskType == REDUCE {
				err := os.Rename(getTempReduceOutput(args.WorkerId, preTaskId),
					getFinalReduceOutput(preTaskId))
				if err != nil {
					log.Fatalf("Failed to rename tempReduceOutputFile to finalReduceOutputFile.\n")
				}
			}

			delete(c.stateTaskMapping, taskId)
			if len(c.stateTaskMapping) == 0 {
				c.gotoNextState()
			}
		}
		c.lock.Unlock()
	}

	task, ok := <-c.unassignedTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign task(%d) to worker(%d).\n", task.Id, args.WorkerId)

	task.WorkerId = args.WorkerId
	task.Deadline = time.Now().Add(10 * time.Second)

	c.stateTaskMapping[assignTaskId(task.Type, task.Id)] = task

	reply.TaskId = task.Id
	reply.TaskType = task.Type
	reply.MapInputFile = task.MapInputFile
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) gotoNextState() {
	if c.stage == MAP {
		log.Printf("All Map tasks have finished, and go to the REDUCE stage.\n")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:       i,
				Type:     REDUCE,
				WorkerId: -1,
			}
			c.stateTaskMapping[assignTaskId(task.Type, task.Id)] = task
			c.unassignedTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("All reduce tasks have finished, and job ends.\n")
		close(c.unassignedTasks)
		c.stage = END
	}
}
