package mr

import (
	"bufio"
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Status struct {
	Slot       int
	WorkerUuid int
	NReduce    int
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	status := &Status{mapf: mapf, reducef: reducef}
	ok := false
	for !ok {
		ok = status.CallRegister()
	}
	go status.iamAlive()
	for {
		taskDetail, hasTask := status.RequestTask()
		if !hasTask {
			break
		}
		status.dealTask(taskDetail)
	}
}

func (a *Status) RequestTask() (*RequestTaskReply, bool) {
	args := &RequestTaskArgs{Slot: a.Slot, WorkerUuid: a.WorkerUuid}
	reply := &RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
		return reply, false
	}
	return reply, true
}

func (a *Status) dealTask(detail *RequestTaskReply) {
	switch detail.Type {
	case MapJob:
		outputs := a.dealMapTask(detail.Inputs[0], detail.TaskId)
		a.submitTaskResult(outputs, detail.TaskId)
	case ReduceJob:
		output := a.dealReduceTask(detail.Inputs, detail.TaskId)
		a.submitTaskResult([]string{output}, detail.TaskId)
	case WaitJob:
		time.Sleep(1 * time.Second)
	}
}

func (a *Status) submitTaskResult(outputs []string, taskId int) bool {
	args := &FinishTaskArgs{Slot: a.Slot, WorkerUuid: a.WorkerUuid, TaskId: taskId, Files: outputs}
	reply := &FinishTaskReply{}

	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return false
	}
	return reply.Ok
}

func (a *Status) dealMapTask(filename string, taskId int) []string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := a.mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	oname := "map-out-"
	output := []string{}

	ofiles := []*os.File{}

	for i := 0; i < a.NReduce; i++ {
		fileName := oname + strconv.Itoa(taskId) + "-" + strconv.Itoa(i) + ".txt"
		output = append(output, fileName)
		ofile, _ := os.Create(fileName)
		defer ofile.Close()
		ofiles = append(ofiles, ofile)
	}

	for _, kv := range intermediate {
		ofile := ofiles[ihash(kv.Key)%a.NReduce]
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}

	return output
}

func (a *Status) dealReduceTask(filenames []string, taskId int) string {
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		fileScanner := bufio.NewScanner(file)
		for fileScanner.Scan() {
			kv := strings.Split(fileScanner.Text(), " ")
			intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	outputName := "mr-out-" + strconv.Itoa(taskId)
	ofile, _ := os.Create(outputName)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := a.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return outputName
}

func (a *Status) iamAlive() {
	for {
		a.SendHeartBeats()
		time.Sleep(1 * time.Second)
	}
}

func (a *Status) SendHeartBeats() {
	args := UniversalArgs{WorkerUuid: a.WorkerUuid, Slot: a.Slot}
	reply := UniversalReply{}

	ok := call("Coordinator.HeartBeat", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func (a *Status) CallRegister() bool {

	args := RegisterArgs{}

	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		a.Slot = reply.Slot
		a.WorkerUuid = reply.WorkerUuid
		a.NReduce = reply.NReduce
	} else {
		fmt.Printf("call failed!\n")
	}
	return ok
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
