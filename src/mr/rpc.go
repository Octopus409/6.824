package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

// Add your RPC definitions here.
type ArgsRequest struct {   // 请求任务的请求体
	X int
}

type ReplyRequest struct {  // 请求任务返回体
	J Job
}

type ArgsDone struct {  // 上传工作的请求体
	J Job
}

type ReplyDone struct {  // 上传工作的返回体
	X int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
