package main

import (
  "fmt"
  "flag"
  "log"
)

var (
  workersCount = flag.Int("n", 1, "number of workers started")
  coordinatorPort = flag.Int("p", 3333, "port that the coordinator listens on")
  dataPath = flag.String("data", "D:/Study/distributed_systems/map_reduce/data", "path to the data on disk")
  linesToRead = flag.Int("tc", 100, "total count of lines to read in the files from data folder")
  batchSize = flag.Int("bs", 20, "batch size of lines to read by each worker")
)

func main() {

  flag.Parse()

  // Creating a slice of workers with the given size (equals capacity)
  workers := make([]*Worker, 0, *workersCount)
  for i := 0; i < *workersCount; i++ {
    w := NewWorker(*dataPath, *coordinatorPort)
    workers = append(workers, w)
  }

  // Create and start a coordinator
  coordinator := NewCoordinator(*coordinatorPort, workers, *linesToRead, *batchSize)
  if err := coordinator.StartCoordinator(); err != nil {
    log.Fatal(err)
  }

  // Starting the workers
  // Learning: be careful with the type of the variables that you loop through --
  // if it is not a pointer, then when calling a goroutine later in the
  // StartWorker method, the callee is being changed to the last
  // worker, hence we call the same goroutine multiple times for the
  // same worker. Cf: https://stackoverflow.com/questions/36121984/how-to-use-a-method-as-a-goroutine-function
  for _, worker := range workers {
    if err := worker.StartWorker(); err != nil {
      log.Fatal(err)
    }
  }

  // Wait for the user input
  fmt.Println("Press the Enter Key to stop anytime")
  fmt.Scanln()
}
