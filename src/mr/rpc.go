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

type Task struct {
	Id      string
	No      int
	Retries int
	Status
	Type
	StartTime time.Time
	Args      []*string
}

type TaskRequest struct {
	Task
}

type Status int

const (
	Pending = iota
	Completed
	Failed
)

type Type int

const (
	Map = iota
	Reduce
	PauseWorker
	StopWorker
)

type TaskResponse struct {
	TaskRequest
	result []*string
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
