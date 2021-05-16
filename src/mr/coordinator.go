package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const sleepDuration = 5000
const retries = 3
const threshold = 10 * time.Second

type state int

const (
	processing = iota
	failed
	completed
)

type requestMessage struct {
	reply chan bool
	*TaskRequest
}

type Coordinator struct {
	state
	inputFiles      []string
	nReducers       int
	requestChannel  chan *chan *Task
	responseChannel chan *TaskResponse
	done            chan bool

	pending           []*Task
	inProcess         map[string]*Task
	completed         map[string]bool
	mrFiles           map[int][]*string
	pendingReductions int
}

func pauseWorkerTask() *Task {
	return &Task{
		Type:      PauseWorker,
		StartTime: time.Now(),
	}
}

func stopWorkerTask() *Task {
	return &Task{
		Type:      StopWorker,
		StartTime: time.Now(),
	}
}

func buildReduceTask(index int, files []*string) *Task {
	return &Task{
		Id:      fmt.Sprintf("r_%d", index),
		No:      index,
		Retries: 0,
		Status:  Pending,
		Type:    Reduce,
		Args:    files,
	}
}

func (c *Coordinator) buildMapTask(index int, file string) *Task {
	s := string(rune(c.nReducers))
	return &Task{
		Id:     fmt.Sprintf("m_%d", index),
		No:     index,
		Status: Pending,
		Type:   Map,
		Args:   []*string{&file, &s},
	}
}

func (c *Coordinator) handleFailedTask(response *TaskResponse) {
	response.Retries++
	if response.Retries <= retries {
		c.pending = append(c.pending, &response.Task)
		return
	}
	c.state = failed
}

func (c *Coordinator) initReduceTasks() []*Task {
	reduceTaskRequests := make([]*Task, len(c.mrFiles))
	for index, files := range c.mrFiles {
		reduceTaskRequests[index] = buildReduceTask(index, files)
	}
	return reduceTaskRequests
}

func (c *Coordinator) saveMappedFiles(response *TaskResponse) {
	for i, file := range response.result {
		if file != nil {
			c.mrFiles[i] = append(c.mrFiles[i], file)
		}
	}
}

func (c *Coordinator) processResponse(response *TaskResponse) {
	switch response.Status {
	case Completed:
		if _, ok := c.completed[response.Id]; !ok {
			delete(c.inProcess, response.Id)
			c.completed[response.Id] = true
		}
		switch response.Type {
		case Map:
			c.saveMappedFiles(response)
			if len(c.pending) == 0 && len(c.inProcess) == 0 {
				c.pending = c.initReduceTasks()
			}
		case Reduce:
			c.pendingReductions--
			if c.pendingReductions == 0 {
				c.state = completed
			}
		}
	default:
		c.handleFailedTask(response)
	}
}

func (c *Coordinator) nextTask() (task *Task) {
	if len(c.pending) > 0 {
		task = c.pending[0]
		c.pending = c.pending[1:]
	} else {
		if c.state == completed || c.state == failed {
			task = stopWorkerTask()
		} else {
			task = pauseWorkerTask()
		}
	}
	return
}

func (c *Coordinator) checkAndMoveTask(request *Task) {
	if time.Now().Sub(request.StartTime) > threshold {
		c.pending = append(c.pending, request)
		delete(c.inProcess, request.Id)
	}
}

func (c *Coordinator) handleLongRunningTasks() {
	for _, request := range c.inProcess {
		c.checkAndMoveTask(request)
	}
}

func (c *Coordinator) manageTasks() {
	for {
		select {
		case reply := <-c.requestChannel:
			*reply <- c.nextTask()
		case response := <-c.responseChannel:
			c.processResponse(response)
			if c.state == completed || c.state == failed {
				c.done <- true
				return
			}
		default:
			time.Sleep(sleepDuration * time.Millisecond)
			c.handleLongRunningTasks()
		}
	}
}

func (c *Coordinator) GetTask(request *TaskRequest) error {
	first := true
	reply := make(chan *Task)
	for {
		select {
		case c.requestChannel <- &reply:
			request.Task = *<-reply
			return nil
		case <-c.done:
			c.done <- true
			request.Task = *stopWorkerTask()
			return nil
		default:
			if first {
				time.Sleep(sleepDuration * time.Millisecond)
				first = false
			} else {
				request.Task = *pauseWorkerTask()
				return nil
			}
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	select {
	case <-c.done:
		c.done <- true
		return true
	default:
		return false
	}
}

func (c *Coordinator) initMapTasks() {
	c.pending = make([]*Task, len(c.inputFiles))
	for i, file := range c.inputFiles {
		c.pending[i] = c.buildMapTask(i, file)
	}
}

func (c *Coordinator) init() {
	c.initMapTasks()
	c.inProcess = make(map[string]*Task)
	c.completed = make(map[string]bool)
	c.mrFiles = make(map[int][]*string)
	c.pendingReductions = c.nReducers
	c.state = processing
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
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles: files,
		nReducers:  nReduce}
	c.init()
	go c.manageTasks()
	c.server()
	return &c
}
