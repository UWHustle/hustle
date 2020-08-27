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

#ifndef PROJECT_THREADING_MUTEX_HPP_
#define PROJECT_THREADING_MUTEX_HPP_

#include <mutex>

#include "utils/macros.h"

template <typename MutexT, bool actually_lock>
class MutexLockImpl {
 public:
  explicit inline MutexLockImpl(MutexT &mutex)  // NOLINT(runtime/references)
      : mutex_ptr_(&mutex) {
    mutex_ptr_->lock();
  }

  inline ~MutexLockImpl() { mutex_ptr_->unlock(); }

 private:
  MutexT *mutex_ptr_;

  DISALLOW_COPY_AND_ASSIGN(MutexLockImpl);
};

template <typename MutexT>
class MutexLockImpl<MutexT, false> {
 public:
  explicit inline MutexLockImpl(MutexT &mutex) {  // NOLINT(runtime/references)
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(MutexLockImpl);
};

using MutexLock = MutexLockImpl<std::mutex, true>;
using RecursiveMutexLock = MutexLockImpl<std::recursive_mutex, true>;

template <bool actually_lock>
using StaticConditionalMutexLock = MutexLockImpl<std::mutex, actually_lock>;
template <bool actually_lock>
using StaticConditionalRecursiveMutexLock =
    MutexLockImpl<std::recursive_mutex, actually_lock>;

#endif  // PROJECT_THREADING_MUTEX_HPP_
