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

import "testing"

func TestIsFileStreamFormat(t *testing.T) {
	tests := []struct {
		format string
		want   bool
	}{
		{format: "orc", want: true},
		{format: "ORC", want: true},
		{format: " parquet ", want: true},
		{format: "csv", want: false},
		{format: "json", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			if got := IsFileStreamFormat(tt.format); got != tt.want {
				t.Fatalf("IsFileStreamFormat(%q) = %t, want %t", tt.format, got, tt.want)
			}
		})
	}
}
