#ifndef PROJECT_UTILITY_EVENT_PROFILER_HPP_
#define PROJECT_UTILITY_EVENT_PROFILER_HPP_

#include <chrono>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "threading/Mutex.hpp"
#include "utils/Macros.hpp"

#include "glog/logging.h"

namespace hustle {

using clock = std::chrono::steady_clock;

template<typename TagT, typename ...PayloadT>
class EventProfiler {
public:
  EventProfiler()
      : zero_time_(clock::now()) {
    global_containers_.reserve(0x1000);
  }

  struct EventInfo {
    clock::time_point start_time;
    clock::time_point end_time;
    bool is_finished;
    std::tuple<PayloadT...> payload;

    explicit EventInfo(const clock::time_point &start_time_in)
        : start_time(start_time_in),
          is_finished(false) {
    }

    EventInfo()
        : start_time(clock::now()),
          is_finished(false) {
    }

    inline void setPayload(PayloadT &&...in_payload) {
      payload = std::make_tuple(in_payload...);
    }

    inline void endEvent() {
      end_time = clock::now();
      is_finished = true;
    }
  };

  struct EventContainer {
    EventContainer() {}

    inline void startEvent(const TagT &tag) {
      events[tag].emplace_back(clock::now());
    }

    inline void endEvent(const TagT &tag) {
      auto &event_info = events.at(tag).back();
      event_info.is_finished = true;
      event_info.end_time = clock::now();
    }

    inline std::vector<EventInfo> *getEventLine(const TagT &tag) {
      return &events[tag];
    }

    std::map<TagT, std::vector<EventInfo>> events;
  };

  EventContainer *getContainer() {
    MutexLock lock(thread_mutex_);
    return &thread_map_[std::this_thread::get_id()];
  }

  EventContainer *getGlobalContainer() {
    MutexLock lock(global_mutex_);
    global_containers_.emplace_back(std::make_unique<EventContainer>());
    return global_containers_.back().get();
  }

  void writeToStream(std::ostream &os) const {
    time_t rawtime;
    time(&rawtime);
    char event_id[32];
    strftime(event_id, sizeof event_id, "%Y-%m-%d %H:%M:%S",
             localtime(&rawtime));

    int thread_id = 0;
    for (const auto &thread_ctx : thread_map_) {
      for (const auto &event_group : thread_ctx.second.events) {
        for (const auto &event_info : event_group.second) {
          CHECK(event_info.is_finished)
          << "Unfinished profiling event at thread " << thread_id
          << ": " << event_group.first;

          os << std::setprecision(12)
             << event_id << ","
             << thread_id << "," << event_group.first << ",";

          PrintTuple(os, event_info.payload, ",");

          os << std::chrono::duration<double>(
              event_info.start_time - zero_time_).count()
             << ","
             << std::chrono::duration<double>(
                 event_info.end_time - zero_time_).count()
             << "\n";
        }
      }
      ++thread_id;
    }
  }

  void summarizeToStream(std::ostream &os) const {
    std::map<TagT, double> time_slots;
    for (const auto &thread_ctx : thread_map_) {
      for (const auto &event_group : thread_ctx.second.events) {
        auto &time_sum = time_slots[event_group.first];
        for (const auto &event_info : event_group.second) {
          time_sum += std::chrono::duration<double>(
              event_info.end_time - event_info.start_time).count();
        }
      }
    }
    for (const auto &pair : time_slots) {
      os << pair.first << ":\t\t" << pair.second << "\n";
    }

    for (const auto &global_ctx : global_containers_) {
      for (const auto &event_group : global_ctx->events) {
        for (const auto &event_info : event_group.second) {
          const double start_time = std::chrono::duration<double>(
              event_info.start_time - zero_time_).count();
          const double end_time = std::chrono::duration<double>(
              event_info.end_time - zero_time_).count();
          os << std::fixed << std::setprecision(3)
             << event_group.first << "["
             << start_time << "," << end_time << "]\n";
        }
      }
    }
  }

  void clear() {
    zero_time_ = clock::now();
    thread_map_.clear();
  }

  const std::map<std::thread::id, EventContainer> &containers() {
    return thread_map_;
  }

  const clock::time_point &zero_time() {
    return zero_time_;
  }

private:
  template<class Tuple, std::size_t N>
  struct TuplePrinter {
    static void
    Print(std::ostream &os, const Tuple &t, const std::string &sep) {
      TuplePrinter<Tuple, N - 1>::Print(os, t, sep);
      os << std::get<N - 1>(t) << sep;
    }
  };

  template<class Tuple>
  struct TuplePrinter<Tuple, 0> {
    static void
    Print(std::ostream &os, const Tuple &t, const std::string &sep) {
    }
  };

  template<class... Args>
  static void PrintTuple(std::ostream &os,
                         const std::tuple<Args...> &t,
                         const std::string &sep) {
    TuplePrinter<decltype(t), sizeof...(Args)>::Print(os, t, sep);
  }

  clock::time_point zero_time_;
  std::map<std::thread::id, EventContainer> thread_map_;
  std::vector<std::unique_ptr<EventContainer>> global_containers_;
  std::mutex thread_mutex_;
  std::mutex global_mutex_;

  DISALLOW_COPY_AND_ASSIGN(EventProfiler);
};

extern EventProfiler<std::string> simple_profiler;

}  // namespace project

#endif  // PROJECT_UTILITY_EVENT_PROFILER_HPP_
