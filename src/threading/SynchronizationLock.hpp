#ifndef PROJECT_THREADING_SYNCHRONIZATION_LOCK_HPP_
#define PROJECT_THREADING_SYNCHRONIZATION_LOCK_HPP_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "utils/Macros.hpp"

class SynchronizationLock {
 public:
  SynchronizationLock() : ready_(false) {}

  void wait() {
    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [this]{ return this->ready_; });
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
