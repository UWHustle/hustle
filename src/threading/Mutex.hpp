#ifndef PROJECT_THREADING_MUTEX_HPP_
#define PROJECT_THREADING_MUTEX_HPP_

#include <mutex>

#include "utility/Macros.hpp"

template <typename MutexT, bool actually_lock>
class MutexLockImpl {
 public:
  explicit inline MutexLockImpl(MutexT &mutex)  // NOLINT(runtime/references)
      : mutex_ptr_(&mutex) {
    mutex_ptr_->lock();
  }

  inline ~MutexLockImpl() {
    mutex_ptr_->unlock();
  }

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

template <bool actually_lock> using StaticConditionalMutexLock
    = MutexLockImpl<std::mutex, actually_lock>;
template <bool actually_lock> using StaticConditionalRecursiveMutexLock
    = MutexLockImpl<std::recursive_mutex, actually_lock>;

#endif  // PROJECT_THREADING_MUTEX_HPP_
