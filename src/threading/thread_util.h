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
