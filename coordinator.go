package main

import (
  "log"
  "fmt"
  "math"
  "net"
  "time"
  "net/http"
  "net/rpc"
)

type Coordinator struct {
  Port int
  WorkersPorts []int
  linesToRead int
  batchSize int

  tasks []Task
  curTasksToWorkersAllocation map[int]int
  reductionResult map[string]int
}

func (c *Coordinator) StartCoordinator() error {

  startLine := 1
  for endLine := c.batchSize; endLine <= c.linesToRead; endLine += c.batchSize {
    c.tasks = append(c.tasks, Task{startLine, endLine})
    startLine += c.batchSize
  }

  rpc.Register(c)
  rpc.HandleHTTP()
  listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", c.Port))
  if err != nil {
    log.Fatal(err)
  }

  go http.Serve(listener, nil)
  go c.serviceLoop()

  return nil
}

func (c *Coordinator) GetWork(args *Args, reply *Task) error {

  for i, task := range c.tasks {
    if _, ok := c.curTasksToWorkersAllocation[i]; !ok {
      *reply = task
      c.curTasksToWorkersAllocation[i] = args.WorkerPort
      break
    }
  }

  return nil
}

func (c *Coordinator) serviceLoop() {
  ticker := time.NewTicker(10 * time.Second)
  for {
    <-ticker.C

    tasksCleared := make([]int, 0, 10)

    // Checking whether the results for the active tasks are available
    for taskID, assignedWorkerPort := range c.curTasksToWorkersAllocation {
      client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", assignedWorkerPort))
      if err == nil {
        args := &Args{c.Port}
        workerResult := NewMapResult()
        if errCall := client.Call("Worker.GetResults", args, &workerResult); errCall != nil {
          log.Fatal(errCall)
        } else {
          if workerResult.Done {
            for text, cnt := range workerResult.Result {
              c.reductionResult[text] += cnt
            }

            tasksCleared = append(tasksCleared, taskID)
          } else {
            fmt.Println("Worker not yet done, need to reassign the task to the other worker...")
            // TODO
          }
        }
      }
    }

    // Removing cleared results from the task list
    for _, taskID := range tasksCleared {
      delete(c.curTasksToWorkersAllocation, taskID)
    }

    // Printing the current accumulated result
    fmt.Println("Reduction results so far:")
    for text, cnt := range c.reductionResult {
      fmt.Printf("%s: %d\n", text, cnt)
    }
  }
}

func NewCoordinator(port int, workers []*Worker, linesToRead int, batchSize int) Coordinator {
  c := Coordinator {
    Port : port,
    WorkersPorts : make([]int, 0, len(workers)),
    linesToRead : linesToRead,
    batchSize : batchSize,
    tasks : make([]Task, 0, int(math.Ceil( float64(linesToRead) / float64(batchSize) ))),
    curTasksToWorkersAllocation : make(map[int]int),
    reductionResult : make(map[string]int),
  }

  for _, worker := range workers {
    c.WorkersPorts = append(c.WorkersPorts, worker.Port)
  }

  return c
}
