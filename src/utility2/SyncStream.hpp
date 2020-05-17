#ifndef PROJECT_UTILITY_SYNC_STREAM_HPP_
#define PROJECT_UTILITY_SYNC_STREAM_HPP_

#include <mutex>
#include <ostream>
#include <unordered_map>

#include "utility2/Macros.hpp"

class SyncStream {
 public:
  explicit SyncStream(std::ostream &os)
      : os_(os), lock_(GetMutex(&os)) {}

  template <typename T>
  inline SyncStream& operator<<(const T &value) {
    os_ << value;
    return *this;
  }

 private:
  std::ostream &os_;
  std::lock_guard<std::mutex> lock_;

  static std::unordered_map<std::ostream*, std::mutex>& MutexTable() {
    static std::unordered_map<std::ostream*, std::mutex> mtable;
    return mtable;
  }

  static std::mutex& MutexTableMutex() {
    static std::mutex mtable_mutex;
    return mtable_mutex;
  }

  static std::mutex& GetMutex(std::ostream *os) {
    std::lock_guard<std::mutex> lock(MutexTableMutex());
    return MutexTable()[os];
  }

  DISALLOW_COPY_AND_ASSIGN(SyncStream);
};

#endif  // PROJECT_UTILITY_SYNC_STREAM_HPP_
