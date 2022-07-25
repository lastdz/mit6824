package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Map = iota
	Reduce
	Sleep
	Quit
)
const (
	Working = iota
	Timeout
)
const (
	NotStarted = iota
	Processing
	Finished
)

type Task struct {
	Name      string
	Type      int
	Status    int
	mFileName string
	rFileName int
}
type Coordinator struct {
	// Your definitions here.
	Taskpoll    map[string]*Task
	Rdone       map[int]int
	Mdone       map[string]int
	Rcount      int
	Mcount      int
	Mid         map[int][]string
	mutex       sync.Mutex
	MapFinished bool
	NeedNumber  int
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
	c.mutex.Lock()
defer c.mutex.Unlock()
	ret := false

	// Your code here.
	if c.Rcount==c.NeedNumber{
	ret=true
	}
	return ret
}

var taskNumber = 0

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapFinished: false,
		mutex:       sync.Mutex{},
		Mdone:       make(map[string]int),
		Rdone:       make(map[int]int),
		Mcount:      0,
		Rcount:      0,
		NeedNumber:  nReduce,
		Taskpoll:    make(map[string]*Task),
		Mid:         make(map[int][]string),
	}
	for _, f := range files {
		c.Mdone[f] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.Rdone[i] = 0
	}
	//fmt.Println("start coo %d",nReduce)
	// Your code here.

	c.server()
	return &c
}
func (c *Coordinator) GetTask(req *Getreq, resp *Getresp) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	resp.ReduceNumber = c.NeedNumber
	resp.TaskName = strconv.Itoa(taskNumber)
	taskNumber++

	if c.MapFinished {
				if c.Rcount==c.NeedNumber{
					resp.TaskType=Quit
					return nil
				}
		for index := 0; index < c.NeedNumber; index++{
			if c.Rdone[index] == Processing || c.Rdone[index] == Finished {
				//fmt.Println("busy",index)
				continue
			}
				//fmt.Println("free",index)
			c.Rdone[index] = Processing
			str := []string{}
			str = append(str, c.Mid[index]...)
			resp.RFileName = str
			resp.TaskType = Reduce
			tsk := &Task{resp.TaskName, Reduce, 0, "", index}
			c.Taskpoll[resp.TaskName] = tsk
			go c.HandleTimeout(resp.TaskName)
			return nil
		}
		resp.TaskType = Sleep
		return nil
	} else {
		for fil, _ := range c.Mdone {
			if c.Mdone[fil] == Processing || c.Mdone[fil] == Finished {
				continue
			}
			c.Mdone[fil] = Processing
			resp.MFileName = fil
			resp.TaskType = Map
			tsk := &Task{resp.TaskName, Map, 0, fil, -1}
			c.Taskpoll[resp.TaskName] = tsk
			go c.HandleTimeout(resp.TaskName)
			return nil
		}
		resp.TaskType = Sleep
		return nil
	}

}
func (c *Coordinator) HandleTimeout(TaskName string) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if tsk, ok := c.Taskpoll[TaskName]; ok {
		tsk.Status = Timeout
		if c.MapFinished {
			if c.Rdone[tsk.rFileName] == Processing {
				c.Rdone[tsk.rFileName] = NotStarted
			}
		} else {
			if c.Mdone[tsk.mFileName] == Processing {
				c.Mdone[tsk.mFileName] = NotStarted
			}

		}
	}
}
func (c *Coordinator) Report(req *ReportStatusRequest, resp *ReportStatusResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	tsk := c.Taskpoll[req.TaskName]
	if tsk.Status == Timeout {
		delete(c.Taskpoll, req.TaskName)
		return nil
	}
	delete(c.Taskpoll, req.TaskName)
	if tsk.Type == Map {
		for _, fil := range req.FilesName {
			index := fil[strings.LastIndex(fil, "_")+1:]
			inde, _ := strconv.Atoi(index)
			c.Mid[inde] = append(c.Mid[inde], fil)
		}
		c.Mdone[tsk.mFileName] = Finished
		c.Mcount++
		//fmt.Println(c.Mcount,len(c.Mdone))
		if c.Mcount == len(c.Mdone) {
			c.MapFinished = true
		}
	} else if tsk.Type == Reduce {
		c.Rdone[tsk.rFileName] = Finished
		c.Rcount++
	}
	return nil
}
