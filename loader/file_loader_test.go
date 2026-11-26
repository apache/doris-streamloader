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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"doris-streamloader/report"

	"github.com/pierrec/lz4/v4"
)

func TestLoadFilesPreservesBinaryContents(t *testing.T) {
	want := []byte("ORC\x00\xff\n\x80binary-footer")
	file := createTempSourceFile(t, want)

	var got []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		got, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		fmt.Fprint(w, `{"Status":"Success","NumberTotalRows":3,"NumberLoadedRows":3,"LoadBytes":20}`)
	}))
	t.Cleanup(server.Close)

	streamLoad, loadResp := newFileTestStreamLoad(server.URL)
	retryInfo := map[int]int{}
	streamLoad.LoadFiles([]*os.File{file}, &retryInfo)
	streamLoad.wg.Wait()

	if !bytes.Equal(got, want) {
		t.Fatalf("request body was modified:\n got: %v\nwant: %v", got, want)
	}
	if loadResp.TotalRows != 3 {
		t.Fatalf("TotalRows = %d, want 3", loadResp.TotalRows)
	}
}

func TestLoadFilesSendsEachFileAsASeparateRequest(t *testing.T) {
	wants := [][]byte{
		[]byte("ORC-first-file"),
		[]byte("ORC-second-file"),
	}
	files := []*os.File{
		createTempSourceFile(t, wants[0]),
		createTempSourceFile(t, wants[1]),
	}

	var (
		mu   sync.Mutex
		gots [][]byte
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		mu.Lock()
		gots = append(gots, body)
		mu.Unlock()
		fmt.Fprint(w, `{"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	t.Cleanup(server.Close)

	streamLoad, _ := newFileTestStreamLoad(server.URL)
	retryInfo := map[int]int{}
	streamLoad.LoadFiles(files, &retryInfo)
	streamLoad.wg.Wait()

	if len(gots) != len(wants) {
		t.Fatalf("request count = %d, want %d", len(gots), len(wants))
	}
	remaining := make(map[string]int, len(wants))
	for _, want := range wants {
		remaining[string(want)]++
	}
	for _, got := range gots {
		remaining[string(got)]--
	}
	for content, count := range remaining {
		if count != 0 {
			t.Fatalf("request body %q count delta = %d, want 0", content, count)
		}
	}
}

func TestLoadFilesRetriesOnlySelectedFiles(t *testing.T) {
	files := []*os.File{
		createTempSourceFile(t, []byte("ORC-first-file")),
		createTempSourceFile(t, []byte("ORC-second-file")),
		createTempSourceFile(t, []byte("ORC-third-file")),
	}

	var got []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		got, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		fmt.Fprint(w, `{"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	t.Cleanup(server.Close)

	streamLoad, _ := newFileTestStreamLoad(server.URL)
	retryInfo := map[int]int{1: 1}
	streamLoad.LoadFiles(files, &retryInfo)
	streamLoad.wg.Wait()

	if want := []byte("ORC-second-file"); !bytes.Equal(got, want) {
		t.Fatalf("retried request body = %q, want %q", got, want)
	}
	if gotWorkers := streamLoad.report.TotalWorkers; gotWorkers != 1 {
		t.Fatalf("TotalWorkers = %d, want 1", gotWorkers)
	}
}

func TestLoadFilesCompressesBinaryContentsWhenEnabled(t *testing.T) {
	want := []byte("ORC\x00\xff\n\x80binary-footer")
	file := createTempSourceFile(t, want)

	var (
		got      []byte
		readErr  error
		encoding string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		got, readErr = io.ReadAll(lz4.NewReader(r.Body))
		fmt.Fprint(w, `{"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	t.Cleanup(server.Close)

	streamLoad, _ := newFileTestStreamLoad(server.URL)
	streamLoad.Compress = true
	retryInfo := map[int]int{}
	streamLoad.LoadFiles([]*os.File{file}, &retryInfo)
	streamLoad.wg.Wait()

	if readErr != nil {
		t.Fatalf("decompress request body: %v", readErr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decompressed request body = %v, want %v", got, want)
	}
	if encoding != "lz4" {
		t.Fatalf("Content-Encoding = %q, want lz4", encoding)
	}
}

func TestLoadFilesPreservesBinaryContentsAfterRedirect(t *testing.T) {
	want := []byte("ORC\x00\xff\n\x80binary-footer")
	file := createTempSourceFile(t, want)

	var got []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		got, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read redirected request body: %v", err)
		}
		fmt.Fprint(w, `{"Status":"Success","NumberTotalRows":1,"NumberLoadedRows":1}`)
	}))
	t.Cleanup(backend.Close)

	frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", backend.URL+r.URL.Path)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(frontend.Close)

	streamLoad, _ := newFileTestStreamLoad(frontend.URL)
	retryInfo := map[int]int{}
	streamLoad.LoadFiles([]*os.File{file}, &retryInfo)
	streamLoad.wg.Wait()

	if !bytes.Equal(got, want) {
		t.Fatalf("redirected request body = %v, want %v", got, want)
	}
}

func createTempSourceFile(t *testing.T, content []byte) *os.File {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "source-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(content); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func newFileTestStreamLoad(serverURL string) (*StreamLoad, *Resp) {
	loadResp := &Resp{}
	reporter := report.NewReporter(1, 1, 1)
	pool := &sync.Pool{New: func() interface{} { return make([]byte, 0, 1024) }}
	streamLoad := NewStreamLoad(
		serverURL,
		"db",
		"table",
		"user",
		"password",
		map[string]string{"format": "orc"},
		nil,
		pool,
		StreamLoadOption{CheckUTF8: true, Timeout: 5},
		reporter,
		loadResp,
	)
	return streamLoad, loadResp
}
