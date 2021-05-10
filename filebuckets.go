package main

import (
  "log"
  "errors"
  "strconv"
  "strings"
  "io/ioutil"
)

type IntervalOfLines struct {
  Begin int
  End int
}

type FileBuckets struct {
  linesIntervals []IntervalOfLines
}

func (fb FileBuckets) GetFilesIntervals(begin int, end int) []IntervalOfLines {
  files := make([]IntervalOfLines, 0, 2)

  for _, linesInterval := range fb.linesIntervals {
    if (begin > linesInterval.End) || (end < linesInterval.Begin) {
      continue
    }

    files = append(files, IntervalOfLines{linesInterval.Begin, linesInterval.End})
  }

  return files
}

func parseFileName(fileName string) (IntervalOfLines, error) {
  splittedFirst := strings.Split(fileName, ".")
  if len(splittedFirst) < 2 {
    return IntervalOfLines{}, errors.New("incorrect filename format")
  } else {
    splittedSecond := strings.Split(splittedFirst[0], "-")
    if len(splittedSecond) != 2 {
      return IntervalOfLines{}, errors.New("interval is not fully reflected in the filename")
    } else {

      beg, err := strconv.Atoi(splittedSecond[0])
      if err != nil {
        log.Fatal(err)
      }
      end, err := strconv.Atoi(splittedSecond[1])
      if err != nil {
        log.Fatal(err)
      }

      return IntervalOfLines{beg, end}, nil
    }
  }
}

func NewFileBuckets(dirPath string) FileBuckets {

  fb := FileBuckets{
    linesIntervals : make([]IntervalOfLines, 0, 100),
  }

  files, err := ioutil.ReadDir(dirPath)
  if err != nil {
    log.Fatal(err)
  }

  for _, file := range files {
    interval, err := parseFileName(file.Name())
    if err != nil {
      log.Fatal(err)
    }

    fb.linesIntervals = append(fb.linesIntervals, interval)
  }

  return fb
}
