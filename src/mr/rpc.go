package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type Task struct {
	Id           int
	Type         string
	MapInputFile string
	WorkerId     int
	Deadline     time.Time
}

type RequestTaskArgs struct {
	WorkerId    int
	PreTaskId   int
	PreTaskType string
}

type RequestTaskReply struct {
	TaskId       int
	TaskType     string
	MapInputFile string
	NMap         int
	NReduce      int
}

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
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
