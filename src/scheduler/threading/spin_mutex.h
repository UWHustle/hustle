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

#ifndef PROJECT_THREADING_SPIN_MUTEX_HPP_
#define PROJECT_THREADING_SPIN_MUTEX_HPP_

#include <atomic>

#include "threading/mutex.h"
#include "utils/macros.h"

namespace hustle {

class SpinMutex {
 public:
  SpinMutex() : locked_(false) {}

  inline void lock() {
    bool previous_locked = false;
    while (!locked_.compare_exchange_weak(previous_locked, true,
                                          std::memory_order_acquire,
                                          std::memory_order_relaxed)) {
      previous_locked = false;
    }
  }

  inline void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> locked_;

  DISALLOW_COPY_AND_ASSIGN(SpinMutex);
};

typedef MutexLockImpl<SpinMutex, true> SpinMutexLock;
template <bool actually_lock>
using StaticConditionalSpinMutexLock = MutexLockImpl<SpinMutex, actually_lock>;

}  // namespace hustle

#endif  // PROJECT_THREADING_SPIN_MUTEX_HPP_
