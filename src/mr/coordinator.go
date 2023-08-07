package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	FileName string
	IdMap    int
	IdReduce int
}

type Coordinator struct {
	// Your definitions here.
	State         int // 0 Map, 1 Reduce, 2 Finish
	NumMapTask    int
	NumReduceTask int
	MapTaskFin    chan bool
	ReduceTaskFin chan bool
	MapTask       chan Task
	ReduceTask    chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	if len(c.MapTaskFin) != c.NumMapTask && len(c.MapTask) != 0 {
		maptask, ok := <-c.MapTask
		if ok {
			reply.XTask = maptask
		}
		reply.CurNumMapTask = len(c.MapTask)
		reply.CurNumReduceTask = len(c.ReduceTask)
	} else {
		reply.CurNumMapTask = -1
		reply.CurNumReduceTask = len(c.ReduceTask)
	}
	if c.State == 1 {
		if len(c.ReduceTask) != 0 {
			reducetask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reducetask
			}
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = len(c.ReduceTask)
		} else {
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = -1
		}
	}

	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	if len(c.MapTaskFin) != c.NumMapTask {
		c.MapTaskFin <- true
		if len(c.MapTaskFin) == c.NumMapTask {
			c.State = 1
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		c.ReduceTaskFin <- true
	}
	time.Sleep(time.Second)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.ReduceTaskFin) == c.NumReduceTask {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:         0,
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		MapTaskFin:    make(chan bool, len(files)),
		ReduceTaskFin: make(chan bool, nReduce)}
	// Your code here.

	for id, file := range files {
		c.MapTask <- Task{FileName: file, IdMap: id}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{IdReduce: i}
	}

	c.server()
	return &c
}
