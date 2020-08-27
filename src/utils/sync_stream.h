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

#ifndef PROJECT_UTILITY_SYNC_STREAM_HPP_
#define PROJECT_UTILITY_SYNC_STREAM_HPP_

#include <mutex>
#include <ostream>
#include <unordered_map>

#include "utils/macros.h"

class SyncStream {
 public:
  explicit SyncStream(std::ostream& os) : os_(os), lock_(GetMutex(&os)) {}

  template <typename T>
  inline SyncStream& operator<<(const T& value) {
    os_ << value;
    return *this;
  }

 private:
  std::ostream& os_;
  std::lock_guard<std::mutex> lock_;

  static std::unordered_map<std::ostream*, std::mutex>& MutexTable() {
    static std::unordered_map<std::ostream*, std::mutex> mtable;
    return mtable;
  }

  static std::mutex& MutexTableMutex() {
    static std::mutex mtable_mutex;
    return mtable_mutex;
  }

  static std::mutex& GetMutex(std::ostream* os) {
    std::lock_guard<std::mutex> lock(MutexTableMutex());
    return MutexTable()[os];
  }

  DISALLOW_COPY_AND_ASSIGN(SyncStream);
};

#endif  // PROJECT_UTILITY_SYNC_STREAM_HPP_
