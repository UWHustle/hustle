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

#ifndef PROJECT_THREADING_THREAD_UTIL_HPP_
#define PROJECT_THREADING_THREAD_UTIL_HPP_

//#include "ProjectConfig.h"

#ifdef PROJECT_HAVE_PTHREAD_SETAFFINITY_NP_LINUX
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif
#include <pthread.h>
#include <sched.h>
#endif

#include "glog/logging.h"

inline static void BindToCPU(const int cpu_id) {
#ifdef PROJECT_HAVE_PTHREAD_SETAFFINITY_NP_LINUX
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);

  if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
    LOG(FATAL) << "Failed to pin thread to CPU " << cpu_id;
  }
#endif
}

#endif  // PROJECT_THREADING_THREAD_UTIL_HPP_
