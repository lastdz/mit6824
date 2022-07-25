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
type Getreq struct {
	X int
}
type Getresp struct {
	MFileName    string   //map文件名字
	TaskName     string   //任务名字
	RFileName    []string //reduce文件名字
	TaskType     int      //0:map,1:reduce,2:sleep
	ReduceNumber int
}
type ReportStatusRequest struct {
	FilesName []string //告诉master，中间文件的名字，reduce用不上
	TaskName  string
	//FileName string//output file name
}
type ReportStatusResponse struct {
	X int
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
