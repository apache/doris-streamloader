<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Doris Streamloader

A robust, high-performance and user-friendly alternative to the traditional curl-based Stream Load.



## Key Features

- **Parallel Loading**: Split data files automatically and perform parallel loading
- **Support for Multiple Files and Directories**: Support multiple files and directories load with one shot
- **Path Traversal Support**: Support path traversal when the source files are in directories
- **Resilience and Continuity**: Resume loading from previous failures and cancellations
- **Automatic Retry Mechanism**: Retry automatically when failure
- **Comprehensive and Concise Input Parameters**



## Usage

```shell
doris-streamloader --source_file={FILE_LIST} --url={FE_OR_BE_SERVER_URL}:{PORT} --header={STREAMLOAD_HEADER} --db={TARGET_DATABASE} --table={TARGET_TABLE}
```

- `FILE_LIST`: directory or file list, support \* wildcard
- `FE_OR_BE_SERVER_URL` & `PORT`: Doris FE or BE hostname or IP and HTTP port
- `STREAMLOAD_HEADER`: supports all headers as `curl` Stream Load does，multiple headers are separated by '?'
- `TARGET_DATABASE` & `TARGET_TABLE`: indicate the target database and table where the data will be loaded

e.g.:

```shell
doris-streamloader --source_file="data.csv" --url="http://localhost:8330" --header="column_separator:|?columns:col1,col2" --db="testdb" --table="testtbl"
```

For additional details and options, refer to our comprehensive docs below.



## Docs

[User Guide](https://doris.apache.org/docs/ecosystem/doris-streamloader)

[中文使用文档](https://doris.apache.org/zh-CN/docs/ecosystem/doris-streamloader)



## Build

To build Streamloader, ensure you have golang installed (version >= 1.19.9). For example, on CentOS:

```
yum install golang
```

Then, navigate to the doris-streamloader directory and execute:

```
cd doris-streamloader && sh build.sh
```



## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
