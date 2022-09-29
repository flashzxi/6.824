package mr

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct {
	uid        int
	Slot       int
	WorkerUuid int
	Status     TaskStatus
	input      []string
	outputs    []string
	startTime  time.Time
}

type LeaderStatue int

const (
	DoMap LeaderStatue = iota
	WaitForMapFinish
	MapFinsh
	DoReduce
	WaitForReduceFinish
	WaitForLastReduce
	Finish
)

type TaskStatus int

const (
	RemainMapTask TaskStatus = iota
	OnGoingMapTask
	FinishedMapTask
	RemainReduceTask
	OnGoingReduceTask
	FinishedReduceTask
	TheLastTask
	WaitTask
)

type Coordinator struct {
	mutex              sync.Mutex
	statusLocker       sync.RWMutex
	nReduce            int
	slotVolumn         int
	availableSlot      []int
	slots              []*WorkerStatus
	leaderStatus       LeaderStatue
	RemainMapTask      list.List
	OnGoingMapTask     list.List
	FinishedMapTask    list.List
	RemainReduceTask   list.List
	OnGoingReduceTask  list.List
	FinishedReduceTask list.List
}

type WorkerStatus struct {
	Alive       bool
	WorkerUuid  int
	TaskElement *list.Element
	locker      *sync.RWMutex
}

func (a *WorkerStatus) checkUuid(uuid int) bool {
	return uuid == a.WorkerUuid
}

func (a *WorkerStatus) setAlive(alive bool) {
	a.locker.Lock()
	a.Alive = alive
	a.locker.Unlock()
}

func (a *WorkerStatus) isAlive() bool {
	return a.Alive
}

func (a *WorkerStatus) isTaskMatch(taskId int) bool {
	return a.TaskElement.Value.(*Task).uid == taskId
}

func (a *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	slot, uuid := args.Slot, args.WorkerUuid
	if a.slots[slot].isAlive() && a.slots[slot].checkUuid(uuid) {
		// 分配 Map 任务
		if a.leaderStatus == DoMap || a.leaderStatus == WaitForMapFinish {
			mapTask := a.DistributeMapTask()
			if mapTask == nil {
				reply.Type = WaitJob
				return nil
			}
			mapTask.startTime = time.Now()
			mapTask.Slot = slot
			mapTask.WorkerUuid = uuid
			mapTask.Status = OnGoingMapTask
			uid := rand.Int()
			mapTask.uid = uid
			reply.TaskId = uid
			reply.Inputs = mapTask.input
			reply.Type = MapJob
			a.slots[slot].TaskElement = a.OnGoingMapTask.PushBack(mapTask)
			return nil
		} else if a.leaderStatus == DoReduce || a.leaderStatus == WaitForReduceFinish || a.leaderStatus == WaitForLastReduce {
			reduceTask := a.DistributeReduceTask()
			if reduceTask == nil {
				reply.Type = WaitJob
				return nil
			}
			reduceTask.startTime = time.Now()
			reduceTask.Slot = slot
			reduceTask.WorkerUuid = uuid
			if a.leaderStatus == WaitForLastReduce {
				reduceTask.Status = TheLastTask
				uid := rand.Int()
				reduceTask.uid = uid
				reply.TaskId = uid
			} else {
				reduceTask.Status = OnGoingReduceTask
				reply.TaskId = reduceTask.uid
			}
			reply.Inputs = reduceTask.input
			reply.Type = ReduceJob
			a.slots[slot].TaskElement = a.OnGoingReduceTask.PushBack(reduceTask)
			return nil
		} else {
			reply.Type = WaitJob
			return nil
		}
	} else {
		return fmt.Errorf("slot: %d and workerUuid: %d doesn't match", slot, uuid)
	}
}

func (a *Coordinator) finishReduce() {
	// inputFiles := make([]string, 0)
	// for e := a.FinishedReduceTask.Front(); e != nil; e = e.Next() {
	// 	inputFiles = append(inputFiles, e.Value.(*Task).outputs...)
	// }
	// a.mergeFiles(inputFiles)
	a.leaderStatus = Finish
}

func (a *Coordinator) mergeFiles(inputFiles []string) {
	// do nothing
	intermediate := []KeyValue{}
	for _, filename := range inputFiles {
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

	outputName := "mr-correct-wc.txt"
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
		output := sumString(values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func sumString(values []string) int {
	var sum int = 0
	for _, value := range values {
		i, _ := strconv.Atoi(value)
		sum += i
	}
	return sum
}

func (a *Coordinator) finishMap() {
	a.statusLocker.Lock()
	a.leaderStatus = DoReduce
	a.statusLocker.Unlock()
	reduceTask := make([]*Task, a.nReduce)
	inputFiles := make([]string, 0)
	for e := a.FinishedMapTask.Front(); e != nil; e = e.Next() {
		inputFiles = append(inputFiles, e.Value.(*Task).outputs...)
	}

	inputs := make([][]string, a.nReduce)
	for _, filename := range inputFiles {
		index := strings.Split(filename, ".")
		index = strings.Split(index[0], "-")

		i, _ := strconv.Atoi(index[len(index)-1])
		inputs[i] = append(inputs[i], filename)
	}
	for i := 0; i < len(reduceTask); i++ {
		reduceTask[i] = &Task{Status: RemainReduceTask, input: inputs[i], uid: i}
		a.RemainReduceTask.PushBack(reduceTask[i])
	}
}

func (a *Coordinator) FinishTask(args *FinishTaskArgs, replay *FinishTaskReply) error {
	slot, workerUuid, taskId, files := args.Slot, args.WorkerUuid, args.TaskId, args.Files
	if a.slots[slot] != nil && a.slots[slot].checkUuid(workerUuid) && a.slots[slot].isTaskMatch(taskId) {
		taskElement := a.slots[slot].TaskElement
		task := taskElement.Value.(*Task)
		switch task.Status {
		case OnGoingMapTask:
			task.Status = FinishedMapTask
			task.outputs = files
			a.OnGoingMapTask.Remove(taskElement)
			a.FinishedMapTask.PushBack(task)
			if a.OnGoingMapTask.Len() == 0 && a.RemainMapTask.Len() == 0 {
				a.finishMap()
			}
		case OnGoingReduceTask:
			task.Status = FinishedReduceTask
			task.outputs = files
			a.OnGoingReduceTask.Remove(taskElement)
			a.FinishedReduceTask.PushBack(task)
			if a.OnGoingReduceTask.Len() == 0 && a.RemainReduceTask.Len() == 0 {
				a.finishReduce()
			}
		default:
			return fmt.Errorf("error task status : %d", task.Status)
		}
		return nil
	} else {
		return fmt.Errorf("task doesn't match")
	}
}

func (a *Coordinator) DistributeMapTask() *Task {
	mapTaskElement := a.RemainMapTask.Front()
	if mapTaskElement == nil {
		mapTaskElement = a.OnGoingMapTask.Front()
		if mapTaskElement == nil {
			return nil
		}
		if mapTaskElement.Value.(*Task).isTimeOut() {
			return a.OnGoingMapTask.Remove(mapTaskElement).(*Task)
		} else {
			a.leaderStatus = WaitForMapFinish
			return nil
		}
	}
	return a.RemainMapTask.Remove(mapTaskElement).(*Task)
}

func (a *Coordinator) DistributeReduceTask() *Task {
	reduceTaskElement := a.RemainReduceTask.Front()
	if reduceTaskElement == nil {
		reduceTaskElement = a.OnGoingReduceTask.Front()
		if reduceTaskElement == nil {
			return nil
		}
		if reduceTaskElement.Value.(*Task).isTimeOut() {
			return a.OnGoingReduceTask.Remove(reduceTaskElement).(*Task)
		} else {
			a.leaderStatus = WaitForReduceFinish
			return nil
		}
	}
	return a.RemainReduceTask.Remove(reduceTaskElement).(*Task)
}

func (a *Coordinator) DistributeWaitTask() {

}

func (a *Task) isTimeOut() bool {
	return time.Since(a.startTime).Seconds() > 10
}

// 串行执行
func (a *Coordinator) getSlot() int {
	a.mutex.Lock()
	if len(a.availableSlot) == 0 {
		for i := a.slotVolumn; i < 2*a.slotVolumn; i++ {
			a.availableSlot = append(a.availableSlot, i)
		}
	}
	a.slotVolumn *= 2
	slotIndex := a.availableSlot[0]
	a.availableSlot = a.availableSlot[1:]
	a.mutex.Unlock()
	return slotIndex
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.Slot = c.getSlot()
	reply.WorkerUuid = time.Now().Nanosecond()
	for len(c.slots) < reply.Slot+1 {
		// fmt.Printf("slots length: %d, Replay.Slot: %d\n", len(c.slots), reply.Slot)
		c.slots = append(c.slots, &WorkerStatus{locker: &sync.RWMutex{}})
	}
	locker := &sync.RWMutex{}
	locker.Lock()
	c.slots[reply.Slot].locker = locker
	c.slots[reply.Slot].Alive = true
	c.slots[reply.Slot].WorkerUuid = reply.WorkerUuid
	locker.Unlock()
	reply.NReduce = c.nReduce
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
	go c.checkWorkersAlive()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.statusLocker.Lock()
	ret := c.leaderStatus == Finish
	c.statusLocker.Unlock()
	return ret
}

func (a *Coordinator) HeartBeat(args *UniversalArgs, reply *UniversalReply) error {
	slot := args.Slot
	if slot > a.slotVolumn {
		return fmt.Errorf(
			"slot out of boundary: slot: %d, slotVolumn: %d",
			slot,
			a.slotVolumn)
	}

	uuid := args.WorkerUuid
	status := a.slots[slot]
	if status.checkUuid(uuid) {
		status.setAlive(true)
		// fmt.Printf("receive heatbeats from %v\n", uuid)
		return nil
	} else {
		return fmt.Errorf("workerUUuid doesn't match, work may expireded. WorkerUuid: %d", uuid)
	}
}

// 判断 worker 是否下线
func (a *Coordinator) checkWorkersAlive() {
	for _, workerStatus := range a.slots {
		if workerStatus != nil {
			workerStatus.setAlive(false)
		}
	}

	time.Sleep(3 * time.Second)
	for index, workerStatus := range a.slots {
		if workerStatus != nil && !workerStatus.isAlive() {
			workerStatus = nil
			a.availableSlot = append(a.availableSlot, index)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	slots := make([]*WorkerStatus, 2)
	slots[0] = &WorkerStatus{Alive: false, locker: &sync.RWMutex{}}
	slots[1] = &WorkerStatus{Alive: false, locker: &sync.RWMutex{}}
	c := Coordinator{
		slotVolumn:    2,
		availableSlot: []int{0, 1},
		slots:         slots,
		nReduce:       nReduce,
		mutex:         sync.Mutex{},
		statusLocker:  sync.RWMutex{},
	}
	for _, file := range files {
		c.RemainMapTask.PushBack(&Task{Status: RemainMapTask, input: []string{file}})
	}

	c.server()
	return &c
}
