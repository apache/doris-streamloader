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

package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"doris_streamloader/loader"
	"doris_streamloader/reader"
	"doris_streamloader/report"
	"doris_streamloader/utils"

	log "github.com/sirupsen/logrus"
)

const (
	fileBufferSize              = 16 * 1024 * 1024 // 16MB
	bufferSize                  = 1 * 1024 * 1024  // 1MB
	queueSize                   = 50 * 1024 * 1024 //50MB
	defaultTimeout              = 60 * 60 * 10
	defaultBatchRows            = 4096
	defaultBatchBytes           = 943718400
	defaultMaxBytesPerTask      = 107374182400
	defaultReportDuration       = 1
	defaultMaxRetryTimes        = 3
	defaultRetryInterval        = 60
	defaultDiskThroughput       = 800 // 800MB/s
	defaultStreamLoadThroughput = 100 // 100MB/s
)

var (
	sourceFilePaths string

	url       string
	dbName    string
	tableName string
	userName  string
	password  string
	compress  bool
	header    string
	headers   map[string]string

	timeout int

	batchRows            int
	batchBytes           int
	maxRowsPerTask       int
	maxBytesPerTask      int
	debug                bool
	workers              int
	enableConcurrency    bool
	diskThroughput       int
	streamLoadThroughput int
	checkUTF8            bool
	reportDuration       int
	retry                string
	maxRetryTimes        int
	retryInterval        int
	retryInfo            map[int]int
	showVersion          bool

	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, bufferSize)
		},
	}
	loadInfo  *loader.LoadInfo
	loadResp  *loader.Resp
	startTime time.Time
)

func initFlags() {
	flag.StringVar(&sourceFilePaths, "source_file", "", "source file paths")
	flag.StringVar(&url, "url", "", "url")
	flag.StringVar(&dbName, "db", "", "db name")
	flag.StringVar(&tableName, "table", "", "table name")
	flag.StringVar(&userName, "u", "root", "username")
	flag.StringVar(&password, "p", "", "password")
	flag.StringVar(&header, "header", "", "header")
	flag.BoolVar(&compress, "compress", false, "compress")
	flag.IntVar(&timeout, "timeout", defaultTimeout, "connect/read/write timeout seconds for rw and wait fe/be header") // 10h
	flag.IntVar(&batchRows, "batch", defaultBatchRows, "batch row size")
	flag.IntVar(&batchBytes, "batch_byte", defaultBatchBytes, "batch byte size")
	flag.IntVar(&maxBytesPerTask, "max_byte_per_task", defaultMaxBytesPerTask, "max byte per task") // 100G
	flag.IntVar(&workers, "workers", 0, "workers")
	flag.IntVar(&diskThroughput, "disk_throughput", defaultDiskThroughput, "disk throughput")
	flag.IntVar(&streamLoadThroughput, "streamload_throughput", defaultStreamLoadThroughput, "estimate streamload throughput")
	flag.BoolVar(&checkUTF8, "check_utf8", true, "check utf8")
	flag.IntVar(&reportDuration, "report_duration", defaultReportDuration, "report duration") // 1s
	flag.StringVar(&retry, "auto_retry", "", "retry failure")
	flag.IntVar(&maxRetryTimes, "auto_retry_times", defaultMaxRetryTimes, "retry failure")
	flag.IntVar(&retryInterval, "auto_retry_interval", defaultRetryInterval, "retry failure")
	flag.BoolVar(&debug, "debug", false, "enable debug")
	flag.BoolVar(&showVersion, "version", false, "Display the version")

	flag.Parse()

	paramCheck()

	loadInfo = &loader.LoadInfo{
		SourceFilePaths:      sourceFilePaths,
		Url:                  url,
		DbName:               dbName,
		TableName:            tableName,
		UserName:             userName,
		Password:             password,
		Compress:             compress,
		Headers:              headers,
		Timeout:              timeout,
		BatchRows:            batchRows,
		BatchBytes:           batchBytes,
		MaxBytesPerTask:      maxBytesPerTask,
		Debug:                debug,
		Workers:              workers,
		DiskThroughput:       diskThroughput,
		StreamLoadThroughput: streamLoadThroughput,
		CheckUTF8:            checkUTF8,
		ReportDuration:       reportDuration,
		NeedRetry:            true,
		RetryTimes:           maxRetryTimes,
		RetryInterval:        retryInterval,
	}

	loadResp = &loader.Resp{
		Status:         "Success",
		TotalRows:      0,
		FailLoadRows:   0,
		LoadedRows:     0,
		FilteredRows:   0,
		UnselectedRows: 0,
		LoadBytes:      0,
		LoadTimeMs:     0,
		LoadFiles:      []string{},
	}

	logLevel := "info"
	// debug print flags
	if debug {
		logLevel = "debug"

		fmt.Println("source_file: ", sourceFilePaths)
		fmt.Println("url: ", url)
		fmt.Println("db: ", dbName)
		fmt.Println("table: ", tableName)
		fmt.Println("username: ", userName)
		fmt.Println("password: ", password)
		// print headers
		for k, v := range headers {
			fmt.Printf("header: %s:%s\n", k, v)
		}
		fmt.Println("compress: ", compress)
		fmt.Println("timeout: ", timeout)
		fmt.Println("batch_row_size: ", batchRows)
		fmt.Println("batch_byte_size: ", batchBytes)
		fmt.Println("max_rows_per_task: ", maxRowsPerTask)
		fmt.Println("max_bytes_per_task: ", maxBytesPerTask)
		fmt.Println("debug: ", debug)
		fmt.Println("workers: ", workers)
		fmt.Println("check_utf8: ", checkUTF8)
		fmt.Println("report_duration: ", reportDuration)
		fmt.Println("retry_info: ", retry)
		fmt.Println("retry_times: ", maxRetryTimes)
		fmt.Println("retry_interval: ", retryInterval)
	}

	utils.InitLog(logLevel)
}

func paramCheck() {
	if showVersion {
		var err error
		commitHash, err := getCommitHash()
		if err != nil {
			log.Warn("Failed to get commit info", err)
		}
		version, err := getTag()
		if err != nil {
			log.Warn("Failed to get version info", err)
		}
		fmt.Printf("version %s, git commit %s\n", version, commitHash)
		os.Exit(0)
	}

	// split retry "a,b;c,d" into {a:b; c:d}
	retryInfo = make(map[int]int)
	if retry != "" {
		for _, v := range strings.Split(retry, ";") {
			kv := strings.Split(v, ",")
			workerIndex, err := strconv.ParseInt(kv[0], 20, 64)
			if err != nil {
				log.Errorf("bad retry info, err %v", err)
				os.Exit(1)
			}
			taskIndex, err := strconv.ParseInt(kv[1], 20, 64)
			if err != nil {
				log.Errorf("bad retry info, err %v", err)
				os.Exit(1)
			}
			retryInfo[int(workerIndex)] = int(taskIndex)
		}
	}

	// check url
	if url == "" {
		log.Errorf("url is empty")
		os.Exit(1)
	}

	// check source file path
	if sourceFilePaths == "" {
		log.Errorf("source file path is empty")
		os.Exit(1)
	}

	if dbName == "" {
		log.Errorf("db name is empty")
		os.Exit(1)
	}

	if tableName == "" {
		log.Errorf("table name is empty")
		os.Exit(1)
	}

	// split header "a:b?c:d" into {a:b, c:d}
	if header != "" {
		headers = make(map[string]string)
		for _, v := range strings.Split(header, "?") {
			if v == "" {
				continue
			}
			kv := strings.Split(v, ":")
			if strings.ToLower(kv[0]) == "format" && strings.ToLower(kv[1]) == "csv" {
				enableConcurrency = true
			}
			headers[kv[0]] = kv[1]
		}
	}

	if timeout <= 0 {
		log.Warnf("timeout invalid: %d, replace with default value: %d", timeout, defaultTimeout)
		timeout = defaultTimeout
	}

	if batchRows <= 0 {
		log.Warnf("batchRows invalid: %d, replace with default value: %d", batchRows, defaultBatchRows)
		batchRows = defaultBatchRows
	}

	if batchBytes <= 0 {
		log.Warnf("batchBytes invalid: %d, replace with default value: %d", batchBytes, defaultBatchBytes)
		batchBytes = defaultBatchBytes
	}

	if reportDuration <= 0 {
		log.Warnf("reportDuration invalid: %d, replace with default value: %d", reportDuration, defaultReportDuration)
		reportDuration = defaultReportDuration
	}

	if maxBytesPerTask <= 0 {
		log.Warnf("maxBytesPerTask invalid: %d, replace with default value: %d", maxBytesPerTask, defaultMaxBytesPerTask)
		maxBytesPerTask = defaultMaxBytesPerTask
	}

	if maxRetryTimes < 0 {
		log.Warnf("maxRetryTimes invalid: %d, replace with default value: %d", maxRetryTimes, defaultMaxRetryTimes)
		maxRetryTimes = defaultMaxRetryTimes
	}

	if retryInterval < 0 {
		log.Warnf("retryInterval invalid: %d, replace with default value: %d", retryInterval, defaultRetryInterval)
		retryInterval = defaultRetryInterval
	}
}

func calculateAndCheckWorkers(reader *file.FileReader, size int64) {
	if workers > 0 {
		return
	}

	if !enableConcurrency {
		loadInfo.Workers = 1
		return
	}

	ratio := float64(size) / float64(maxBytesPerTask)
	tmpWorkers := 0

	if ratio > 0.0 && ratio <= 0.001 {
		tmpWorkers = 1
	} else if ratio > 0.001 && ratio <= 0.01 {
		tmpWorkers = 2
	} else if ratio > 0.01 && ratio <= 0.1 {
		tmpWorkers = 4
	} else {
		tmpWorkers = 8
	}
	tmpWorkers = int(math.Min(float64(tmpWorkers), float64(diskThroughput)/float64(streamLoadThroughput)))

	log.Infof("worker number is %d, which is <= 0, trigger automatic inference. Final worker number is %d", workers, tmpWorkers)
	workers = tmpWorkers
	loadInfo.Workers = workers
}

func createStreamLoad(report *report.Reporter, queues []chan []byte) *loader.StreamLoad {
	// create StreamLoadOption && StreamLoad from flags
	streamLoadOption := loader.StreamLoadOption{
		Compress:  compress,
		CheckUTF8: checkUTF8,
		Timeout:   timeout,
	}
	return loader.NewStreamLoad(url, dbName, tableName, userName, password, headers, queues, &bufferPool, streamLoadOption, report, loadResp)
}

func createQueues(queues *[]chan []byte) {
	for i := 0; i < workers; i++ {
		*queues = append(*queues, make(chan []byte, queueSize))
	}
}

func getCommitHash() (string, error) {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	outputBytes, err := cmd.Output()
	if err != nil {
		return "", err
	}

	commitHash := string(outputBytes)
	commitHash = strings.TrimSpace(commitHash)

	return commitHash, nil
}

func getTag() (string, error) {
	cmd := exec.Command("git", "describe", "--tags", "--abbrev=0")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	tag := strings.TrimSpace(string(output))
	return tag, nil
}

func main() {
	initFlags()
	retryCount := 0
	for {
		// create queue by worker size
		var queues []chan []byte
		// create file reader
		fileSize := int64(0)
		reader := file.NewFileReader(sourceFilePaths, batchRows, batchBytes, fileBufferSize, &queues, &bufferPool, &fileSize)
		calculateAndCheckWorkers(reader, fileSize)
		createQueues(&queues)
		reporter := report.NewReporter(reportDuration, fileSize, uint64(workers))
		streamLoad := createStreamLoad(reporter, queues)
		if retryCount == 0 {
			startTime = time.Now()
		}

		maxRowsPerTask = math.MinInt32
		streamLoad.Load(workers, maxRowsPerTask, maxBytesPerTask, &retryInfo)
		reporter.Report()
		defer reporter.CloseWait()
		reader.Read(reporter, workers, maxBytesPerTask, &retryInfo, loadResp, retryCount)
		reader.Close()

		streamLoad.Wait(loadInfo, retryCount, &retryInfo, startTime)
		if !loadInfo.NeedRetry || retryCount >= loadInfo.RetryTimes-1 {
			break
		}
		time.Sleep(time.Duration(loadInfo.RetryInterval) * time.Second)
		retryCount++
	}
}
