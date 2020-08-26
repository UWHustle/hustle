#ifndef PROJECT_THREADING_SPIN_MUTEX_HPP_
#define PROJECT_THREADING_SPIN_MUTEX_HPP_

#include <atomic>

#include "threading/Mutex.hpp"
#include "utils/Macros.hpp"

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
