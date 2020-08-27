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

#ifndef PROJECT_THREADING_SYNCHRONIZATION_LOCK_HPP_
#define PROJECT_THREADING_SYNCHRONIZATION_LOCK_HPP_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "utils/macros.h"

class SynchronizationLock {
 public:
  SynchronizationLock() : ready_(false) {}

  void wait() {
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [this] { return this->ready_; });
  }

  void release() {
    std::lock_guard<std::mutex> lock(m_);
    ready_ = true;
    cv_.notify_all();
  }

  static std::shared_ptr<SynchronizationLock> CreateShared() {
    return std::make_shared<SynchronizationLock>();
  }

 private:
  std::condition_variable cv_;
  std::mutex m_;
  bool ready_;

  DISALLOW_COPY_AND_ASSIGN(SynchronizationLock);
};

#endif  // PROJECT_THREADING_SYNCHRONIZATION_LOCK_HPP_
