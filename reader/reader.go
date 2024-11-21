// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package file

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	loader "doris-streamloader/loader"
	report "doris-streamloader/report"

	log "github.com/sirupsen/logrus"
)

type FileReader struct {
	batchRows  int
	batchBytes int
	bufferSize int
	queues     *[]chan []byte
	files      []*os.File
	pool       *sync.Pool
}

// create FileReader
func NewFileReader(filePaths string, batchRows int, batchBytes int, bufferSize int, queues *[]chan []byte, pool *sync.Pool, size *int64) *FileReader {
	var files []*os.File

	// parse path
	paths := strings.Split(filePaths, ",")
	for _, filePath := range paths {
		matches, err := filepath.Glob(filePath)
		if len(matches) == 0 {
			log.Errorf("There is no file, file path: %s", filePath)
			os.Exit(1)
		}
		if err != nil {
			log.Errorf("Source file pattern match error, error message : %v", err)
			os.Exit(1)
		}

		for _, match := range matches {
			fileInfo, err := os.Stat(match)
			if err != nil {
				log.Errorf("Get file info error, error message : %v", err)
				os.Exit(1)
			}

			if fileInfo.IsDir() {
				err = filepath.Walk(match, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					if !info.IsDir() {
						file, err := os.Open(path)
						if err != nil {
							return err
						}
						files = append(files, file)
						*size += info.Size()
					}
					return nil
				})
				if err != nil {
					log.Errorf("Failed to traverse directory tree, error message : %v", err)
					os.Exit(1)
				}
			} else {
				file, err := os.Open(match)
				if err != nil {
					log.Errorf("Failed to open file, error message : %v", err)
					os.Exit(1)
				}
				files = append(files, file)
				*size += fileInfo.Size()
			}
		}
	}

	return &FileReader{
		batchRows:  batchRows,
		batchBytes: batchBytes,
		bufferSize: bufferSize,
		queues:     queues,
		files:      files,
		pool:       pool,
	}
}

// Read File
func (f *FileReader) Read(reporter *report.Reporter, workers int, maxBytesPerTask int, retryInfo *map[int]int,
	loadResp *loader.Resp, retryCount int, lineDelimiter byte) {
	index := 0
	data := f.pool.Get().([]byte)
	count := f.batchRows
	// sendBytesMap used to record how many bytes a task has sent and whether it has ended
	// currentTaskMap used to record current task number
	// record these two to determine which data needs to be reload by using currentTaskMap[workerNumber] >= taskIndex
	sendBytesMap := make(map[int]int)
	currentTaskMap := make(map[int]int)
	for i := 0; i < workers; i++ {
		sendBytesMap[i] = maxBytesPerTask
		currentTaskMap[i] = 1
	}

	for _, file := range f.files {
		loadResp.LoadFiles = append(loadResp.LoadFiles, file.Name())
		reader := bufio.NewReaderSize(file, f.bufferSize)

		for {
			if atomic.LoadUint64(&reporter.FinishedWorkers) == atomic.LoadUint64(&reporter.TotalWorkers) {
				return
			}
			line, err := reader.ReadBytes(lineDelimiter)
			if err == io.EOF && len(line) == 0 {
				file.Close()
				break
			} else if err != nil {
				log.Errorf("Read file failed, error message: %v, before retrying, we suggest:\n1.Check the input data files and fix if there is any problem.\n2.Do select count(*) to check whether data is partially loaded.\n3.If the data is partially loaded and duplication is unacceptable, consider dropping the table (with caution that all data in the table will be lost) and retry.\n4.Otherwise, just retry.\n", err)
				if len(line) !=0 {
					log.Error("5.When using a specified line delimiter, the file must end with that delimiter.")
				}
				os.Exit(1)
			}

			// copy line into data
			data = append(data, line...)

			count--
			if retryCount == 0 {
				loadResp.TotalRows++
			}
			if count == 0 || len(data) > f.batchBytes {
				reporter.Lock.Lock()
				_, needSkipWorker := reporter.FailedWorkers[index%workers]
				reporter.Lock.Unlock()
				// if the worker is not failed
				if !needSkipWorker {
					needSkipTask := false
					// need retry
					if len(*retryInfo) > 0 {
						// judge if need skip data of task
						taskIndex, isRetry := (*retryInfo)[index%workers]
						if !(isRetry && currentTaskMap[index%workers] >= taskIndex) {
							needSkipTask = true
						}
					}
					// if do not need skip task, send data to queue
					if !needSkipTask {
						f.sendToQueue(reporter, data, index%workers)
					}
					// update reporter
					reporter.IncrSendBytes(int64(len(data)))
					// if sendBytes less than 0,
					// add task number and reset send byte to maxBytesPerTask
					sendBytesMap[index%workers] -= len(data)
					if sendBytesMap[index%workers] <= 0 {
						sendBytesMap[index%workers] = maxBytesPerTask
						currentTaskMap[index%workers] += 1
					}
				}
				data = f.pool.Get().([]byte)
				count = f.batchRows
				index++
			}
		}
	}
	// send remain data
	if len(data) > 0 {
		reporter.IncrSendBytes(int64(len(data)))
		(*f.queues)[index%workers] <- data
	}
}

func (f *FileReader) sendToQueue(reporter *report.Reporter, data []byte, workerIndex int) {
	end := false
	for {
		select {
		case (*f.queues)[workerIndex] <- data:
			end = true
		case <-time.After(time.Second * 5):
			reporter.Lock.Lock()
			_, skip := reporter.FailedWorkers[workerIndex]
			reporter.Lock.Unlock()
			if skip {
				end = true
			}
		}
		if end {
			return
		}
	}
}

// close FileReader
func (f *FileReader) Close() {
	for _, queue := range *f.queues {
		close(queue)
	}
}
