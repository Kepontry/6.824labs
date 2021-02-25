package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Waiting         = 1
	Processing      = 2
	Done            = 3
	TimeOutDuration = 10 * time.Second
)

var mutex sync.Mutex

type TaskInterface interface {
	GenTaskInfo(taskInfo *TaskInfo)
}
type Master struct {
	// Your definitions here.
	nReduce     int
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	IsDone      bool
	MasterLock sync.Mutex
}
type Task struct {
	BeginTime time.Time
	Status    int
	Order     int
}
type MapTask struct {
	Task
	Filename string
}
type ReduceTask struct {
	Task
}

func (task *Task) TimeOut() bool {
	return time.Now().Sub(task.BeginTime) > TimeOutDuration
}
func (mapTask *MapTask) GenTaskInfo(taskInfo *TaskInfo) {
	taskInfo.TaskKind = TaskKindMap
	taskInfo.TaskOrder = mapTask.Order
	taskInfo.FileName = mapTask.Filename

}
func (reduceTask *ReduceTask) GenTaskInfo(taskInfo *TaskInfo) {
	taskInfo.TaskKind = TaskKindReduce
	taskInfo.TaskOrder = reduceTask.Order
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	reply.Z = 2
	return nil
}

func (m *Master) AssignWork(args *TaskInfo, reply *TaskInfo) error {
	if m.IsDone {
		reply.TaskKind = TaskKindEnd
		return nil
	}
	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()
	TaskCompleted := 0
	for index, mapTask := range m.MapTasks {
		switch mapTask.Status {
		case Waiting:
			m.MapTasks[index].Status = Processing
			//mapTask.Status = Processing
			reply.ReduceTasksSum = m.nReduce
			reply.MapTasksSum = len(m.MapTasks)
			mapTask.GenTaskInfo(reply)
			return nil
		case Done:
			TaskCompleted += 1
		default:
			continue
		}
	}

	// map tasks are not all completed, so wait
	if TaskCompleted < len(m.MapTasks) {
		reply.TaskKind = TaskKindWait
		return nil
	}

	TaskCompleted = 0
	for index, reduceTask := range m.ReduceTasks {
		switch reduceTask.Status {
		case Waiting:
			m.ReduceTasks[index].Status = Processing
			//reduceTask.Status = Processing
			reply.ReduceTasksSum = m.nReduce
			reply.MapTasksSum = len(m.MapTasks)
			reduceTask.GenTaskInfo(reply)
			return nil
		case Done:
			TaskCompleted += 1
		default:
			continue
		}
	}
	// map tasks are not all completed, so wait
	if TaskCompleted < len(m.ReduceTasks) {
		reply.TaskKind = TaskKindWait
		return nil
	}
	m.IsDone = true
	return nil
}
func (m *Master) WorkDone(args *TaskInfo, reply *TaskInfo) error {
	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()

	switch args.TaskKind {
	case TaskKindMap:
		m.MapTasks[args.TaskOrder].Status = Done
		println("Master receive done message, fileName:", args.FileName)
	case TaskKindReduce:
		m.ReduceTasks[args.TaskOrder].Status = Done
		println("Master receive done message, taskOrder:", args.TaskOrder)
	default:
		log.Fatalf("bad TaskKind %v", args.TaskKind)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	return m.IsDone
}
func (m *Master) CheckTimeOut() {
	for {
		m.MasterLock.Lock()
		for index, mapTask := range m.MapTasks {
			if mapTask.Status == Processing && mapTask.TimeOut() {
				m.MapTasks[index].Status = Waiting //lock
				//mapTask.Status = Waiting
			}
		}
		for index, reduceTask := range m.ReduceTasks {
			if reduceTask.Status == Processing && reduceTask.TimeOut() {
				m.ReduceTasks[index].Status = Waiting //lock
			}
		}
		m.MasterLock.Unlock()
		time.Sleep(time.Second)
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.IsDone = false
	for index, filename := range files {
		TempTask := MapTask{
			Task: Task{
				BeginTime: time.Now(),
				Status:    Waiting,
				Order:     index,
			},
			Filename: filename,
		}
		//todo init
		//m.MapTasks =
		m.MapTasks = append(m.MapTasks, TempTask)
	}
	for i := 0; i < nReduce; i++ {
		TempTask := ReduceTask{
			Task{
				BeginTime: time.Now(),
				Status:    Waiting,
				Order:     i,
			},
		}
		//todo init
		m.ReduceTasks = append(m.ReduceTasks, TempTask)
	}
	// Your code here.

	m.server()
	go m.CheckTimeOut()
	return &m
}
