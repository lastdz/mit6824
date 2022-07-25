package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func HandleMap(mapf func(string, string) []KeyValue, filename string, filenum int, tasknum string) []string {
	file, ok := os.Open(filename)
	if ok != nil {
		log.Fatal("fuck")
	}

	content, _ := ioutil.ReadAll(file)
	file.Close()
	filenames := make([]string, filenum)
	kva := mapf(filename, string(content))
	files := make([]*os.File, 0)
	nam := "mr_"
	for i := 0; i < filenum; i++ {
		filenames[i] = nam + tasknum + "_" + strconv.Itoa(i)
		file, _ := os.Create(nam + tasknum + "_" + strconv.Itoa(i))
		files = append(files, file)
	}
	for _, kv := range kva {
		has := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[has])
		enc.Encode(&kv)
	}
	return filenames
}
func HandleReduce(reducef func(string, []string) string, filenames []string) string {
	files := make([]*os.File, len(filenames))
	mid := []KeyValue{}
	for index, name := range filenames {
		files[index], _ = os.Open(name)

		kv := KeyValue{}
		dec := json.NewDecoder(files[index])
		for dec.Decode(&kv) == nil {
			mid = append(mid, kv)
		}
	}
	sort.Sort(ByKey(mid))
	oname := "mr-out-" + filenames[0][strings.LastIndex(filenames[0], "_")+1:]
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(mid) {
		j := i + 1
		for j < len(mid) && mid[j].Key == mid[i].Key {
			j++
		}
		value := []string{}
		for h := i; h < j; h++ {
			value = append(value, mid[h].Value)
		}
		tmp := reducef(mid[i].Key, value)
		fmt.Fprintf(ofile, "%v %v\n", mid[i].Key, tmp)
		i = j
	}
	return oname
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		req := &Getreq{1}
		resp := &Getresp{}
		call("Coordinator.GetTask", req, resp)
		if resp.TaskType == Map {
			fils := HandleMap(mapf, resp.MFileName, resp.ReduceNumber, resp.TaskName)
			reqq := &ReportStatusRequest{fils, resp.TaskName}
			respp := &ReportStatusResponse{}
			call("Coordinator.Report", reqq, respp)
			//fmt.Println("worker done Map")
		} else if resp.TaskType == Reduce {
			HandleReduce(reducef, resp.RFileName)
			reqq := &ReportStatusRequest{nil, resp.TaskName}
			respp := &ReportStatusResponse{}
			call("Coordinator.Report", reqq, respp)
			//fmt.Println("worker done Reduce")
		} else if resp.TaskType == Sleep {
			time.Sleep(10 * time.Second)
			//fmt.Println("worker done Sleep")
		} else{
			//fmt.Println("worker done QUit")
			return
		}
		
		time.Sleep(time.Second/10)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
