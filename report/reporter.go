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

package report

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Reporter impl
type Reporter struct {
	// atomic counter byteSends
	sendBytes       int64
	reportDuration  time.Duration
	closeCh         chan struct{}
	wg              sync.WaitGroup
	fileSize        int64
	TotalWorkers    uint64
	FinishedWorkers uint64
	FailedWorkers   map[int]int
	Lock            sync.Mutex
}

func NewReporter(reportDuration int, size int64, totalWorkers uint64) *Reporter {
	return &Reporter{
		reportDuration:  time.Duration(reportDuration),
		closeCh:         make(chan struct{}),
		fileSize:        size,
		TotalWorkers:    totalWorkers,
		FinishedWorkers: 0,
		FailedWorkers:   make(map[int]int),
		Lock:            sync.Mutex{},
	}
}

// incr sent bytes
func (r *Reporter) IncrSendBytes(bytes int64) {
	atomic.AddInt64(&r.sendBytes, bytes)
}

// convert bytes to human readable
func humanReadableBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}

	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}

	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/1024/1024)
	}

	return fmt.Sprintf("%.2f GB", float64(bytes)/1024/1024/1024)
}

// ticker to print send bytes, duration
func (r *Reporter) Report() {
	r.wg.Add(1)
	go r.report()
}

func getProgressBar(progress, total int, fill *int) string {
	barSize := 60 // length of the progress bar
	percentage := float64(progress) / float64(total)
	numberOfBars := int(percentage * float64(barSize))
	bars := ""
	for i := 0; i < numberOfBars; i++ {
		bars += "="
	}
	*fill = numberOfBars
	return bars
}

// fill the rest of the progress bar
func getEmptyBar(emptySize int) string {
	emptyBars := ""
	for i := 0; i < emptySize; i++ {
		emptyBars += " "
	}
	return emptyBars
}

func (r *Reporter) report() {
	ticker := time.NewTicker(r.reportDuration * time.Second)
	beg := time.Now()
	last_sent_time := beg
	last_sent := atomic.LoadInt64(&r.sendBytes)
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			beg_duration := now.Sub(beg).Seconds()
			duration := now.Sub(last_sent_time).Seconds()
			last_sent_time = now

			sent := atomic.LoadInt64(&r.sendBytes)
			rate := int64(float64(sent-last_sent) / duration)
			total := r.fileSize
			progress := int(float64(float64(sent)/float64(total)) * 100)
			if progress >= 100 {
				progress = 100
			}
			last_sent = sent
			fill := 0

			fmt.Print("\r")
			fmt.Printf("Progress: [%s%s] %d%% Elapsed: %.2fs Rate: %s/s",
				getProgressBar(progress, 100, &fill),
				getEmptyBar(60-fill),
				progress,
				beg_duration,
				humanReadableBytes(rate))
			fmt.Print("\033[0K")
			// some worker may be failed.
			if progress == 100 || atomic.LoadUint64(&r.FinishedWorkers) == atomic.LoadUint64(&r.TotalWorkers) {
				fmt.Print("\nFinishing and publishing data ...\n")
				r.wg.Done()
				return
			}

		case <-r.closeCh:
			r.wg.Done()
			return
		}
	}
}

// close and wait for all report done
func (r *Reporter) CloseWait() {
	close(r.closeCh)
	r.wg.Wait()
}
