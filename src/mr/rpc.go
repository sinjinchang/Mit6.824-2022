package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type TaskRequest struct {
	X int
}

type TaskResponse struct {
	XTask            Task
	NumMapTask       int
	NumReduceTask    int
	Id               int
	CurNumMapTask    int
	CurNumReduceTask int
	State            int
}

type TaskFinRequest struct {
	X             int
	NumMapTask    int
	NumReduceTask int
}

type TaskFinResponse struct {
	XTask         Task
	NumMapTask    int
	NumReduceTask int
	Id            int
	MapTaskFin    chan bool
	ReduceTaskFin chan bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
