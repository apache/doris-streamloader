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

package loader

import (
	"doris-streamloader/report"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/pierrec/lz4/v4"
	log "github.com/sirupsen/logrus"
)

type StreamLoadOption struct {
	Compress  bool
	CheckUTF8 bool
	Timeout   int
}

type StreamLoad struct {
	url       string
	dbName    string
	tableName string
	userName  string
	password  string
	headers   map[string]string

	queues []chan []byte
	pool   *sync.Pool
	wg     sync.WaitGroup
	report *report.Reporter

	loadResp *Resp
	StreamLoadOption
}

// Stream load response
type StreamLoadResp struct {
	TxnID                  int    `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int    `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	BeginTxnTimeMs         int    `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
	ErrorURL               string `json:"ErrorURL"`
}

type Resp struct {
	Status         string
	TotalRows      uint64
	FailLoadRows   uint64
	LoadedRows     uint64
	FilteredRows   uint64
	UnselectedRows uint64
	LoadBytes      uint64
	LoadTimeMs     int64
	LoadFiles      []string
}

type ReadOption struct {
	maxBytesPerTask int
	workerIndex     int
	taskIndex       int
}

type LoadInfo struct {
	SourceFilePaths      string
	Url                  string
	DbName               string
	TableName            string
	UserName             string
	Password             string
	Compress             bool
	Headers              map[string]string
	Timeout              int
	BatchRows            int
	BatchBytes           int
	MaxBytesPerTask      int
	Debug                bool
	Workers              int
	DiskThroughput       int
	StreamLoadThroughput int
	CheckUTF8            bool
	ReportDuration       int
	NeedRetry            bool
	RetryTimes           int
	RetryInterval        int
}

// create stream load from flags
func NewStreamLoad(url, dbName, tableName, userName, password string, headers map[string]string, queues []chan []byte, pool *sync.Pool, option StreamLoadOption, report *report.Reporter, loadResp *Resp) *StreamLoad {
	return &StreamLoad{
		url:              url,
		dbName:           dbName,
		tableName:        tableName,
		userName:         userName,
		password:         password,
		headers:          headers,
		queues:           queues,
		pool:             pool,
		wg:               sync.WaitGroup{},
		report:           report,
		loadResp:         loadResp,
		StreamLoadOption: option,
	}
}

// stream load create url
func (s *StreamLoad) createUrl() string {
	return fmt.Sprintf("%s/api/%s/%s/_stream_load", s.url, s.dbName, s.tableName)
}

// stream load create http request with string data
func (s *StreamLoad) createRequest(url string, reader io.Reader, workerIndex int, taskIndex int) (req *http.Request, err error) {
	req, err = http.NewRequest("PUT", url, reader)
	if err != nil {
		return
	}

	// set auth
	req.SetBasicAuth(s.userName, s.password)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "text/plain")
	for k, v := range s.headers {
		req.Header.Set(k, v)
		// If a label has already been set in the headers, to prevent conflicts,
		//generate a unique label by combining the original label, worker index, and task index.
		if k == "label" {
			var builder strings.Builder
			builder.WriteString(v)
			builder.WriteString("_")
			builder.WriteString(strconv.Itoa(workerIndex))
			builder.WriteString("_")
			builder.WriteString(strconv.Itoa(taskIndex))

			req.Header.Set("label", builder.String())
		}

	}

	if s.Compress {
		req.Header.Set("Content-Encoding", "lz4")
		req.Header.Set("compress_type", "LZ4")
	}

	return
}

// read data from queue, write data to pipe pw
func (s *StreamLoad) readData(isEOS *atomic.Bool, rawWriter *io.PipeWriter, readOption *ReadOption) {
	defer rawWriter.Close()

	var writer io.Writer = rawWriter
	if s.Compress {
		// cWriter := gzip.NewWriter(rawWriter)
		cWriter := lz4.NewWriter(rawWriter)

		defer cWriter.Close()
		writer = cWriter
	}

	max_bytes_rows := readOption.maxBytesPerTask

	for {
		if max_bytes_rows <= 0 {
			break
		}

		// get data
		data, ok := <-s.queues[readOption.workerIndex]
		if !ok {
			isEOS.Store(true)
			break
		}

		max_bytes_rows -= len(data)

		// write all data to writer
		// s := string(data)
		// println(s)
		// println("xxxxxxxx")
		// if _, err := writer.Write([]byte(s)); err != nil {
		// 	panic(err)
		// }
		// var a string
		// TODO(Drogon): delete or valid
		var validUTF8Buffer []byte
		if s.CheckUTF8 {
			if utf8.Valid(data) {
				validUTF8Buffer = data
			} else {
				validUTF8Buffer = s.toValidUTF8(data)
				defer s.pool.Put(validUTF8Buffer[:0])
				log.Error("The byte slice is not valid UTF-8 encoding")
			}
		} else {
			validUTF8Buffer = data
		}
		// b := []byte(string([]rune(string(data))))
		// b := []byte(string(data))
		// strings.ToValidUTF8("a\xc5z", "")
		if _, err := writer.Write(validUTF8Buffer); err != nil {
			s.handleSendError(readOption.workerIndex, readOption.taskIndex)
			log.Errorf("send error: %v", err)
			return
		}

		s.pool.Put(data[:0])
	}
}

func (s *StreamLoad) send(url string, reader io.Reader, workerIndex int, taskIndex int) (*http.Response, error) {
	realUrl := url
	for {
		req, err := s.createRequest(realUrl, reader, workerIndex, taskIndex)
		if err != nil {
			if req == nil {
				return nil, err
			} else {
				return req.Response, err
			}
		}

		// create client
		client := &http.Client{
			// max retry 10 times
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return fmt.Errorf("redirect")
			},
			Timeout: time.Duration(s.Timeout) * time.Second,
		}

		// send request
		resp, err := client.Do(req)
		if err != nil {
			return resp, err
		}
		defer resp.Body.Close()

		// read response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Read stream load response failed, response body: %s, error message: %v, it is hard to judge whether load success or not, we suggest:\n1.Do select count(*) to check whether data is partially loaded.\n2.If the data is partially loaded and duplication is unacceptable, consider dropping the table (with caution that all data in the table will be lost) and retry.", body, err)
			return resp, nil
		}

		if resp.StatusCode == 307 {
			realUrl = resp.Header.Get("Location")
			if realUrl == "" {
				log.Errorf("Send error, redirectUrl is empty, error message: %v", err)
				return resp, nil
			}
			log.Infof("redirect to %s", realUrl)
			continue
		}

		// check response
		if resp.StatusCode != 200 {
			// print response headers
			log.Debugf("response headers: %v", resp.Header)
			return resp, fmt.Errorf("response code is not 200, response code: %d, response body: %s", resp.StatusCode, body)
		}

		// parse response
		var respMsg StreamLoadResp
		err = json.Unmarshal(body, &respMsg)
		if err != nil {
			log.Errorf("Parse stream load response failed, response body: %s, error message: %v, it is hard to judge whether load success or not, we suggest:\n1.Do select count(*) to check whether data is partially loaded\n2.If the data is partially loaded and duplication is unacceptable, consider dropping the table (with caution that all data in the table will be lost) and retry.\n", body, err)
			return resp, nil
		}
		if respMsg.Status != "Success" && respMsg.Status != "Publish Timeout" {
			return resp, fmt.Errorf("stream load failed, response %s", body)
		}

		log.Infof("resp: %s", body)

		// update resp
		atomic.AddUint64(&s.loadResp.LoadedRows, uint64(respMsg.NumberLoadedRows))
		atomic.AddUint64(&s.loadResp.FilteredRows, uint64(respMsg.NumberFilteredRows))
		atomic.AddUint64(&s.loadResp.UnselectedRows, uint64(respMsg.NumberUnselectedRows))
		atomic.AddUint64(&s.loadResp.LoadBytes, uint64(respMsg.LoadBytes))

		return resp, nil
	}
}

// convert []byte to utf-8 valid [] byte, if not valid, replace invalid char with U+FFFD
func (s *StreamLoad) toValidUTF8(b []byte) []byte {
	invalid := []byte("\uFFFD")
	buf := s.pool.Get().([]byte)

	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, invalid...)
		} else {
			buf = append(buf, b[:size]...)
		}
		b = b[size:]
	}

	return buf
}

func (s *StreamLoad) handleSendError(workerIndex int, taskIndex int) {
	s.report.Lock.Lock()
	s.report.FinishedWorkers += 1
	s.report.FailedWorkers[workerIndex] = taskIndex
	s.report.Lock.Unlock()
}

// StreamLoad executeGetAndSend: get data from queue and send to server
func (s *StreamLoad) executeGetAndSend(maxRowsPerTask int, maxBytesPerTask int, workerIndex int) {
	defer s.wg.Done()
	defer log.Info("execute worker exit")

	taskIndex := 1

	for {
		url := s.createUrl()
		var isEOS atomic.Bool
		isEOS.Store(false)
		pr, pw := io.Pipe()

		go s.readData(&isEOS, pw,
			&ReadOption{
				maxBytesPerTask: maxBytesPerTask,
				workerIndex:     workerIndex,
				taskIndex:       taskIndex,
			})
		if resp, err := s.send(url, NopCloser(pr), workerIndex, taskIndex); err != nil {
			s.handleSendError(workerIndex, taskIndex)
			log.Errorf("Send error, resp: %v error message: %v", resp, err)
			return
		} else {
			log.Debugf("send success resp: %v", resp)
		}

		if isEOS.Load() {
			break
		}
		taskIndex++
	}
	atomic.AddUint64(&s.report.TotalWorkers, 1)
}

func (s *StreamLoad) ExecuteGetAndSend(maxRowsPerTask int, maxBytesPerTask int, workerIndex int) {
	s.wg.Add(1)
	go s.executeGetAndSend(maxRowsPerTask, maxBytesPerTask, workerIndex)
}

func (s *StreamLoad) Load(workers int, maxRowsPerTask int, maxBytesPerTask int, retryInfo *map[int]int) {
	if len(*retryInfo) > 0 {
		for workerIndex := range *retryInfo {
			s.ExecuteGetAndSend(maxRowsPerTask, maxBytesPerTask, workerIndex)
		}
	} else {
		for i := 0; i < workers; i++ {
			s.ExecuteGetAndSend(maxRowsPerTask, maxBytesPerTask, i)
		}
	}
}

// Wait
func (s *StreamLoad) Wait(loadInfo *LoadInfo, retryCount int, retryInfo *map[int]int, startTime time.Time) {
	s.wg.Wait()

	if len(s.report.FailedWorkers) == 0 {
		s.showLoadResult(true, startTime)
		loadInfo.NeedRetry = false
		return
	}

	// parse header
	var headerStrings []string
	for key, value := range loadInfo.Headers {
		headerStrings = append(headerStrings, fmt.Sprintf("%s:%s", key, value))
	}
	headers := strings.Join(headerStrings, "?")

	if retryCount < loadInfo.RetryTimes-1 {
		log.Infof("\nAuto retrying, retry count %d waiting time %ds...\n", retryCount+1, loadInfo.RetryInterval)
		*retryInfo = make(map[int]int)
		for workerIndex, taskIndex := range s.report.FailedWorkers {
			(*retryInfo)[workerIndex] = taskIndex
		}
		s.report.FailedWorkers = make(map[int]int)
		s.loadResp.LoadFiles = []string{}
		return
	}

	//parse failed worker
	var FailedWorkersStrings []string
	for key, value := range s.report.FailedWorkers {
		FailedWorkersStrings = append(FailedWorkersStrings, fmt.Sprintf("%d,%d", key, value))
	}
	failed := strings.Join(FailedWorkersStrings, ";")

	// retry command
	command := fmt.Sprintf("./doris_streamloader --source_file %s  --url=\"%s\" --header=\"%s\" --db=\"%s\" --table=\"%s\" -u %s -p \"%s\" --compress=%t --timeout=%d --workers=%d --disk_throughput=%d --streamload_throughput=%d --batch=%d --batch_byte=%d --max_byte_per_task=%d --check_utf8=%t --report_duration=%d --auto_retry=\"%s\" --auto_retry_times=%d --auto_retry_interval=%d",
		loadInfo.SourceFilePaths, loadInfo.Url, headers, loadInfo.DbName, loadInfo.TableName, loadInfo.UserName, loadInfo.Password, loadInfo.Compress, loadInfo.Timeout, loadInfo.Workers, loadInfo.DiskThroughput, loadInfo.StreamLoadThroughput, loadInfo.BatchRows, loadInfo.BatchBytes, loadInfo.MaxBytesPerTask, loadInfo.CheckUTF8, loadInfo.ReportDuration, failed, loadInfo.RetryTimes, loadInfo.RetryInterval)

	fmt.Printf("load has some error, and auto retry failed, you can retry by : \n%s\n", command)
	s.showLoadResult(false, startTime)
	loadInfo.NeedRetry = false
}

func (s *StreamLoad) showLoadResult(isSuccessful bool, startTime time.Time) {
	endTime := time.Now()
	elapsed := endTime.Sub(startTime)
	s.loadResp.LoadTimeMs = int64(elapsed.Milliseconds())
	s.loadResp.FailLoadRows = s.loadResp.TotalRows - s.loadResp.LoadedRows - s.loadResp.FilteredRows - s.loadResp.UnselectedRows

	result := "success"
	if !isSuccessful {
		s.loadResp.Status = "Failed"
		result = "fail"
	}

	jsonData, err := json.MarshalIndent(s.loadResp, "", "\t")
	if err != nil {
		log.Errorf("Load %s, but parse load result to json error: %v, can not show load result", result, err)
		return
	}
	fmt.Printf("Load Result: %s\n", jsonData)
}
