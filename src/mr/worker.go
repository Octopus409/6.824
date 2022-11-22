package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "strconv"
import "os"
import "io/ioutil"
import "time"
import "encoding/json"
import "strings"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	alive := true
	for alive {
		reply := CallAskjob()
		switch reply.J.JobType {
		case MapType:
			DoMap(reply,mapf)
			CallDonejob(MapType,reply.J.JobId,reply.J.NReduce)
		case ReduceType:
			DoReduce(reply,reducef)
			CallDonejob(ReduceType,reply.J.JobId,reply.J.NReduce)
		case WaitType:
			fmt.Println("get waiting")
			time.Sleep(time.Second)
		case AllDoneType:
			fmt.Println("all job is done")
			alive = false
		default:
			// 默认等待
			fmt.Println("get waiting")
			time.Sleep(time.Second)
		}
	}
	
}

func DoMap(reply *ReplyRequest,mapf func(string, string) []KeyValue){

	intermediate := []KeyValue{}
	fmt.Println("The JobId is ",reply.J.JobId)
	fmt.Println("The Inputfile len is ",len(reply.J.InputFile))
	kva := mapf("1",reply.J.InputFile[0])  // mapf的第一个参数无意义
	intermediate = append(intermediate, kva...)

	// worker完成排序
	//sort.Sort(ByKey(intermediate))  // 不排序了，留给reduce再排

	// 依照hash，分成nReduce个文件

	// 先创建nReduce个临时string数组
	
	fmt.Println("The nReduce is ",reply.J.NReduce)

	ofile := []*os.File{}
	env := []*json.Encoder{}
	for i:=0; i<reply.J.NReduce; i++{
		fname := "tmp_file/mr-" + strconv.Itoa(reply.J.JobId) + "-" + strconv.Itoa(i)
		file_ptr,err := os.Create(fname)
		encoder_ptr := json.NewEncoder(file_ptr)
		if err!=nil {
			fmt.Printf("Create file %s failed\n",fname)
			os.Exit(-1)
		}
		ofile = append(ofile,file_ptr)
		env = append(env,encoder_ptr)
	}

	for i := 0 ; i < len(intermediate) ; i++{
		index := ihash(intermediate[i].Key)
		index = index % 10;

		env[index].Encode(intermediate[i])  // 使用encode编码进文件
		// this is the correct format for each line of Reduce output.

	}
	
	// 关闭所有文件
	for i:=0; i<reply.J.NReduce; i++{
		ofile[i].Close()
	}

	fmt.Printf("create JobId = %d intermediate file, Reducenumber is %d\n",reply.J.JobId,reply.J.NReduce)
}

func DoReduce(reply *ReplyRequest,reducef func(string, []string) string){
	var str string
	fmt.Printf("The len of Inputfile is %d	",len(reply.J.InputFile))
	for _,s := range reply.J.InputFile{
		str += s  // 把所有inputfile拼接一起
	}
	str2 := strings.NewReader(str)
	dec := json.NewDecoder(str2)
	intermediate := []KeyValue{}
	
	for {
		var u KeyValue
		if err:= dec.Decode(&u); err!=nil{
			break
		}
		intermediate = append(intermediate,u)
	}

	fmt.Printf("The len of intermediate key is %d	",len(intermediate))
	sort.Sort(ByKey(intermediate))

	fname := "tmp_file/mr-out-" + strconv.Itoa(reply.J.JobId)
	ofile,err := os.Create(fname)
	env := json.NewEncoder(ofile)
	if err!=nil {
		fmt.Printf("Create file %s failed",fname)
		os.Exit(-1)
	}
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
		output := reducef(intermediate[i].Key, values)

		tmp_KV := KeyValue{
			Key: intermediate[i].Key,
			Value: output,
		}

		env.Encode(tmp_KV)
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

func CallAskjob() *ReplyRequest{
	args := ArgsRequest{0}
	reply := ReplyRequest{}

	ok := call("Coordinator.Getjob", &args, &reply)
	if ok {
		//fmt.Printf("received a job id=%v\n", reply.J.JobId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func CallDonejob(jobtype int,jobid int,NR int) bool {  // 需要告知工作类型和工作id
	reply := ReplyDone{}
	var args ArgsDone
	args.J = Job{
		JobId:	jobid,
		JobType: jobtype,
		NReduce: NR,
	}
	if jobtype==MapType {
		// 读取相应文件，然后放入args中
		str := Readtmpfile(MapType,jobid,NR)
		args.J.InputFile = str
	} else if jobtype==ReduceType {
		str := Readtmpfile(ReduceType,jobid,NR)
		args.J.InputFile = str
	}

	/*
	fmt.Printf("读取文件成功!\n")
	// 测试一下读取的文件对不对
	for i,file := range args.J.InputFile{
		fmt.Printf("========= Job %d =======\n",i)
		fmt.Println(file[:40])
	}
	*/

	ok := call("Coordinator.JobDone", &args, &reply)
	if ok && reply.X == 1 {
		fmt.Printf("Successfully upload the job %d\n",jobid)
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
	return true
}

func Readtmpfile(jobtype int,jobid int,NReduce int) []string {    // 辅助函数，打开对应文件，返回stirng数组
	ret := []string{}
	if jobtype == MapType {
		for i:=0 ; i<NReduce; i++ {
			// 查看是否存在文件mr-X-i，不存在就放一个空string
			fname := "tmp_file/mr-" + strconv.Itoa(jobid) + "-" + strconv.Itoa(i)
			file,err := os.Open(fname)
			if err != nil {
				log.Fatalf("cannot open %v", fname)
				ret = append(ret,"")
				continue
			}
			content, err := ioutil.ReadAll(file)
			ret = append(ret,string(content))
			file.Close()
		}
	} else if jobtype == ReduceType {
		fname := "tmp_file/mr-out-" + strconv.Itoa(jobid)
		file,err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
			fmt.Printf("cannot open %v",fname)
			os.Exit(-1)
		}
		content,err := ioutil.ReadAll(file)
		ret = append(ret,string(content))
		file.Close()
	}
	return ret
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
