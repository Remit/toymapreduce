package main

type Args struct {
  WorkerPort int
}

type Task struct {
  StartLine int
  EndLine int
}

type MapResult struct {
  Result map[string]int
  Done bool
}

func NewMapResult() MapResult {
  return MapResult {
    Result : make(map[string]int),
    Done : false,
  }
}
