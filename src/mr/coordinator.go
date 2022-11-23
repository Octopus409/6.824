package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io/ioutil"
import "time"
import "fmt"
import "strconv"
import "sync"
import "strings"
import "encoding/json"
import "sort"

var mu sync.Mutex  // 操作coordinator中metaholder的全局锁。

const (
	MapType = iota
	ReduceType
	WaitType
	AllDoneType
)
const (
	MapPhase = iota
	ReducePhase
	AllDone
)
const (
	Waiting = iota
	Working
	Done
)

type Job struct {
	JobType    int   // MapType,ReduceType
	InputFile  []string
	JobId      int 
	NReduce int
	//TmpFileList []string
}

type JobMetaInfo struct {
	condition int   // Waiting Working Done
	StartTime time.Time
	JobPtr    *Job
}

type JobMetaHolder struct {
	MetaMap map[int] *JobMetaInfo
	MetaReduce map[int] *JobMetaInfo
}

func (c *Coordinator) CrashHandler() {   // 定时检测超时worker的协程函数
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.phase == AllDone {
			mu.Unlock()
			continue
		}

		timenow := time.Now()
		if c.phase == MapPhase {
			for _, v := range c.jobMetaHolder.MetaMap {
				if v.condition == Working {
					fmt.Println("Map job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
				}
				if v.condition == Working && time.Now().Sub(v.StartTime) > 5*time.Second {
					fmt.Println("detect a crash on map job ", v.JobPtr.JobId)
					c.mqueue <- v.JobPtr
					v.condition = Waiting
				}
			}
		} else {
			for _, v := range c.jobMetaHolder.MetaReduce {
				if v.condition == Working {
					fmt.Println("Reduce job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
				}
				if v.condition == Working && time.Now().Sub(v.StartTime) > 5*time.Second {
					fmt.Println("detect a crash on reduce job ", v.JobPtr.JobId)
					c.rqueue <- v.JobPtr
					v.condition = Waiting
				}
		   }
		}
		mu.Unlock()
	}

}

func (j *JobMetaHolder) getJobMetaInfo(jobId int,jobtype int) (bool, *JobMetaInfo) {
	if jobtype == MapType {
		res, ok := j.MetaMap[jobId]
		return ok, res
	}
	res, ok := j.MetaReduce[jobId]
	return ok, res
}

func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.condition == Done {
			mapDoneNum ++
		} else {
			mapUndoneNum++
		}
	}
	for _,v := range j.MetaReduce {
		if v.condition == Done {
			reduceDoneNum++
		} else {
			reduceUndoneNum++
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	if (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

func (c *Coordinator) last_work() {
	
	path,_ := os.Getwd()
	path = path + "/main_file/"
	rd, _ := ioutil.ReadDir(path)
	arr := []KeyValue{}
	for _, fi := range rd {  // 遍历路径path下所有文件，凡是符合mr-*-i的文件，读取，然后放入res
		if strings.HasPrefix(fi.Name(), "mr-out-") {
			file,err := os.Open(path+fi.Name())
			if err != nil {
				log.Fatalf("cannot open %v", fi.Name())
			}
			dec := json.NewDecoder(file)
			
			for {
				var u KeyValue
				if dec.Decode(&u)!=nil {
					break
				}
				arr = append(arr,u)
			}
			
			file.Close()
		}
	}
	sort.Sort(ByKey(arr))
	
	// 写入本地磁盘
	fname := "main_file/out"
	file,_ := os.Create(fname)
	env := json.NewEncoder(file)
	for _,kv := range arr {
		env.Encode(kv)
	}
	file.Close()
}

func (c *Coordinator) nextPhase() {
	if c.phase == MapPhase {
		// 重置uniqueJobId
		c.uniqueJobId = 0
		//close(c.JobChannelMap)
		c.MakeReduceJob()
		c.phase = ReducePhase
	} else if c.phase == ReducePhase {
		//close(c.JobChannelReduce)

		// 拼接所有mr-out-Y，然后结束任务
		c.last_work()
		c.phase = AllDone
	}
}

func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	Jobid := JobInfo.JobPtr.JobId
	jobtype := JobInfo.JobPtr.JobType
	if jobtype==MapType {
		meta,_ := j.MetaMap[Jobid]
		if meta != nil{
			fmt.Printf("The Job id=%d has already been insert.",Jobid)
			return false
		} else {
			j.MetaMap[Jobid] = JobInfo
		}
		return true;
	}
	meta,_ := j.MetaReduce[Jobid]
	if meta != nil{
		fmt.Printf("The Job id=%d has already been insert.",Jobid)
		return false
	} else {
		j.MetaReduce[Jobid] = JobInfo
	}
	return true;
	
}

func (j *JobMetaHolder) firetheJob(Jobid int,jobtype int) bool {
	var meta *JobMetaInfo
	if jobtype==MapType{
		meta,_ = j.MetaMap[Jobid]
	} else {
		meta,_ = j.MetaReduce[Jobid]
	}
	if meta==nil{
		fmt.Printf("The Job %d doesn't exist.",Jobid)
		return false
	}
	if meta.condition == Working {
		fmt.Printf("The job %d has been working",Jobid)
		return false
	}
	meta.condition = Working
	meta.StartTime = time.Now()
	return true
}

func (j *JobMetaHolder) printStatus() {
	for _,meta := range j.MetaMap {
		var status string
		switch meta.condition {
		case Waiting:
			status = "Waiting"
		case Working:
			status = "Working"
		case Done:
			status = "Done"
		}
		fmt.Printf("MAPJobid: %d	Status:	%s\n",meta.JobPtr.JobId,status)
	}
	for _,meta := range j.MetaReduce {
		var status string
		switch meta.condition {
		case Waiting:
			status = "Waiting"
		case Working:
			status = "Working"
		case Done:
			status = "Done"
		}
		fmt.Printf("REDUCEJobid: %d	Status:	%s\n",meta.JobPtr.JobId,status)
	}
}

type Coordinator struct {
	// Your definitions here.
	filesname []string
	mqueue chan *Job
	rqueue chan *Job
	NReduce int
	phase int
	uniqueJobId int
	jobMetaHolder JobMetaHolder
}

func (c *Coordinator) generateJobId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
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

func (c *Coordinator)MakeMapJob(files []string){
	for _,fname := range files {
		id := c.generateJobId()  // 得到一个job的id
		// 打开文件，放进InputFile，创建Job、JobMetaInfo
		file,err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fname)
		}
		file.Close()
		thisjob := Job{
			JobType    :MapType,
			InputFile  :[]string{},
			JobId      :id,
			NReduce :c.NReduce,
		}
		thisjob.InputFile = append(thisjob.InputFile,string(content))
		// 创建JobMetaInfo
		thisjobMetaInfo := JobMetaInfo{
			condition :Waiting,   // Waiting Working Done
			JobPtr    :&thisjob,
		}
		c.jobMetaHolder.putJob(&thisjobMetaInfo)
		fmt.Printf("making %d map job\n",id)

		c.mqueue <- &thisjob
	}
}

func (c *Coordinator)MakeReduceJob(){
	for i:=0 ; i<c.NReduce; i++{
		// 将所有Y=i的文件，打包到一个job
		id := c.generateJobId()
		fmt.Println("making reduce job :", id)
		thisjob := Job{
			JobType    :ReduceType,
			InputFile  :[]string{},
			JobId      :id,
			NReduce :c.NReduce,
		}
		thisjob.InputFile = Readmainfile(id)
		thisjobMetaInfo := JobMetaInfo{
			condition :Waiting,   // Waiting Working Done
			JobPtr    :&thisjob,
		}
		c.jobMetaHolder.putJob(&thisjobMetaInfo)

		c.rqueue <- &thisjob
	}
}

func Readmainfile(reduceid int)[]string{
	// 读取指定reduce id的全部文件，并返回一个[]string
	var res []string
	path,_ := os.Getwd()
	path = path + "/main_file/"
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {  // 遍历路径path下所有文件，凡是符合mr-*-i的文件，读取，然后放入res
		if strings.HasPrefix(fi.Name(), "mr-") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceid)) {
			file,err := os.Open(path+fi.Name())
			if err != nil {
				log.Fatalf("cannot open %v", fi.Name())
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fi.Name())
			}
			file.Close()
			res = append(res, string(content))
		}
	}
	return res
}

func (c *Coordinator) Getjob(args *ArgsRequest, reply *ReplyRequest) error {
	mu.Lock()  // 并发执行的任务，需要加锁
	defer mu.Unlock()
	// 从队列取一个任务
	switch c.phase {
	case MapPhase:
		// 分发map任务
		if len(c.mqueue)>0 {
			// 还存在map任务，进行分发和记录
			reply.J  = *<-c.mqueue
			id := reply.J.JobId
	
			c.jobMetaHolder.firetheJob(id,reply.J.JobType)
		} else {
			// 检查状态，看是否需要转变coordinator的phase
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			// 发送等待任务
			reply.J.JobType = WaitType
		}
	case ReducePhase:
		// 分发reduce任务
		if len(c.rqueue) >0 {
			reply.J = *<-c.rqueue
			id := reply.J.JobId

			c.jobMetaHolder.firetheJob(id,reply.J.JobType)
		} else {
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			reply.J.JobType = WaitType
		}
	case AllDone:
		// 全部任务完成
		c.jobMetaHolder.checkJobDone()
		reply.J.JobType = AllDoneType
	}
	

	return nil

}

func (c *Coordinator)JobDone(args *ArgsDone, reply *ReplyDone)error{
	mu.Lock()
	defer mu.Unlock()

	// 判断是map还是reduce
	switch args.J.JobType {
	case MapType:
		_,meta := c.jobMetaHolder.getJobMetaInfo(args.J.JobId,MapType)
		if meta.condition != Working {
			return nil
		}
		for i,str := range args.J.InputFile {
			if str=="" { continue}
			fname := "main_file/mr-" + strconv.Itoa(args.J.JobId) + "-" + strconv.Itoa(i)
			file_ptr,err := os.Create(fname)
			if err!=nil {
				fmt.Printf("Create file %s failed",fname)
				os.Exit(-1)
			}
			fmt.Fprintf(file_ptr,str)  // 把文件写进main_file/下
		}
		reply.X = 1
		// 修改MetaMap
		if meta.condition == Working {
			meta.condition = Done
			return nil
		}
	case ReduceType:
		_,meta := c.jobMetaHolder.getJobMetaInfo(args.J.JobId,ReduceType)
		if meta.condition != Working {
			return nil
		}
		// reduce只有一个文件返回
		fname := "main_file/mr-out-" + strconv.Itoa(args.J.JobId)
		file_ptr,err := os.Create(fname)
		if err!=nil {
			fmt.Printf("Create file %s failed",fname)
			os.Exit(-1)
		}
		fmt.Fprintf(file_ptr,args.J.InputFile[0])
		reply.X = 1
		// 修改MetaMap
		if meta.condition == Working {
			meta.condition = Done
			return nil
		}
	}

	// 接收传过来的mr-X-Y，然后存进磁盘中
	
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
	ret := false

	// Your code here.
	if c.phase == AllDone {
		return true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nR int) *Coordinator {
	c := Coordinator{
		filesname:  files,
		mqueue: make(chan *Job,10),
		rqueue: make(chan *Job,10),
		NReduce: nR,
		phase: MapPhase,
		uniqueJobId: 0,
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)),
			MetaReduce: make(map[int]*JobMetaInfo, nR),
		},
	}

	// Your code here.
	c.MakeMapJob(files)
	

	c.server()

	// 开启监视的协程
	go c.CrashHandler()

	return &c
}
