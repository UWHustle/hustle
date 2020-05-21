#ifndef PROJECT_UTILITY_THREAD_SAFE_QUEUE_HPP_
#define PROJECT_UTILITY_THREAD_SAFE_QUEUE_HPP_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <utility>

#include "../utils/Macros.hpp"

template<typename T>
class ThreadSafeQueue {
public:
  ThreadSafeQueue()
      : num_waiters_(0) {}

  bool empty() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return internal_queue_.empty();
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return internal_queue_.size();
  }

  void clear() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    internal_queue_ = std::queue<T>();
  }

  void push(const T &element) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    internal_queue_.push(element);
    queue_nonempty_condition_.notify_one();
  }

  void push(T &&element) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    internal_queue_.emplace(std::move(element));
    queue_nonempty_condition_.notify_one();
  }

  T popOne() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    if (internal_queue_.empty()) {
      num_waiters_.fetch_add(1, std::memory_order_relaxed);
      queue_nonempty_condition_.wait(
          lock, [&] { return !internal_queue_.empty(); });
      num_waiters_.fetch_sub(1, std::memory_order_relaxed);
    }
    T popped_value(std::move(internal_queue_.front()));
    internal_queue_.pop();
    lock.unlock();
    return popped_value;
  }

  bool popOneIfAvailable(T *destination_ptr) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (internal_queue_.empty()) {
      return false;
    } else {
      *destination_ptr = internal_queue_.front();
      internal_queue_.pop();
      return true;
    }
  }

  std::size_t numWaitingThreads() const {
    return num_waiters_.load(std::memory_order_relaxed);
  }

private:
  std::queue<T> internal_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_nonempty_condition_;
  std::atomic<std::size_t> num_waiters_;

  DISALLOW_COPY_AND_ASSIGN(ThreadSafeQueue);
};

#endif  // PROJECT_UTILITY_THREAD_SAFE_QUEUE_HPP_
