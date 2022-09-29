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

type RegisterArgs struct {
}

type RegisterReply struct {
	Slot       int
	WorkerUuid int
	NReduce    int
}

type UniversalArgs struct {
	Slot       int
	WorkerUuid int
}

type UniversalReply struct {
	Ok bool
}

type RequestTaskArgs UniversalArgs

type RequestTaskReply struct {
	Type   TaskType
	Inputs []string
	TaskId int
}

type FinishTaskArgs struct {
	Slot       int
	WorkerUuid int
	TaskId     int
	Files      []string
}

type FinishTaskReply UniversalReply

type TaskType int

const (
	MapJob TaskType = iota
	ReduceJob
	WaitJob
)

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
