package main

import (
  "fmt"
  "os"
  "log"
  "bufio"
  "math/rand"
  "net"
  "net/rpc"
  "net/http"
)

var (
  minPortNumber = 30000
  maxPortNumber = 60000
)

type Worker struct {
  Port int
  DataPath string
  CoordinatorPort int

  done bool
  curResult map[string]int
  fileBuckets FileBuckets
}


func (w *Worker) StartWorker() error {
  fmt.Println("Hi! ready to work")

  w.fileBuckets = NewFileBuckets(w.DataPath)

  // https://github.com/golang/go/issues/13395
  serv := rpc.NewServer()
  serv.Register(w)

  oldMux := http.DefaultServeMux
  mux := http.NewServeMux()
  http.DefaultServeMux = mux

  serv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

  http.DefaultServeMux = oldMux

  listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", w.Port))
  if err != nil {
    log.Fatal(err)
  }

  go http.Serve(listener, mux)
  go w.work()

  return nil
}

func (w *Worker) GetResults(args *Args, result *MapResult) error {

  resultFromWorker := NewMapResult()
  resultFromWorker.Done = w.done
  for k, v := range w.curResult {
    resultFromWorker.Result[k] = v
  }

  *result = resultFromWorker

  w.done = false
  w.curResult = make(map[string]int)

  return nil
}

func min(a, b int) int {
  if a < b {
    return a
  }

  return b
}

func (w *Worker) work() {

  for {
    var task Task
    args := &Args{w.Port}
    client, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", w.CoordinatorPort))
    if err != nil {
      continue
    }

    if client != nil {
      if err := client.Call("Coordinator.GetWork", args, &task); err != nil {
        log.Fatal(err)
        continue
      }
    }

    fmt.Printf("Got work to do: %d\n", task)

    filesIntervals := w.fileBuckets.GetFilesIntervals(task.StartLine, task.EndLine)
    for _, fileInterval := range filesIntervals {
      skipLinesAtBeginning := 0
      if task.StartLine > fileInterval.Begin {
        skipLinesAtBeginning = task.StartLine - fileInterval.Begin
      }

      readLinesAfterSkip := min(task.EndLine, fileInterval.End) - skipLinesAtBeginning
      fileName := fmt.Sprintf("%d-%d.txt", fileInterval.Begin, fileInterval.End)
      fullName := fmt.Sprintf("%s/%s", w.DataPath, fileName)

      file, err := os.Open(fullName)
      if err != nil {
        log.Fatal(err)
      }
      defer file.Close()

      scanner := bufio.NewScanner(file)
      // Skipping unneeded lines
      for curLine := 0; curLine < skipLinesAtBeginning; curLine++ {
        scanner.Scan()
      }
      // Reading what is needed
      for linesRead := 0; linesRead < readLinesAfterSkip; linesRead++ {
        scanner.Scan()
        textRead := scanner.Text()
        if len(textRead) > 0 {
          w.curResult[textRead]++
        }
      }
    }

    for k, v := range w.curResult {
      fmt.Printf("%s: %d\n", k, v)
    }

    w.done = true

    break
  }
}

func NewWorker(dataPath string, coordinatorPort int) *Worker {
  return &Worker {
    Port : minPortNumber + rand.Intn(maxPortNumber + 1),
    DataPath : dataPath,
    CoordinatorPort : coordinatorPort,
    curResult : make(map[string]int),
  }
}
