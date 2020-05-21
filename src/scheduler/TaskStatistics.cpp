#include "scheduler/TaskStatistics.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <map>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "scheduler/TaskDescription.hpp"

namespace hustle {

void TaskStatistics::summarizePerQueryToStream(std::ostream &os) const {
  std::map<std::size_t, double> time_slots;
  scheduler_.forEachTaskEvent([&](const auto &task_event) {
    const TaskDescription &description = std::get<0>(task_event);
    if (description.getTaskType() == TaskType::kRelationalOperator) {
      const auto &start = std::get<2>(task_event);
      const auto &end = std::get<3>(task_event);
      time_slots[description.getTaskMajorId()] +=
          std::chrono::duration<double>(end - start).count();
    }
  });

  double total = 0;
  for (const auto &it : time_slots) {
    total += it.second;

    os << it.first << ":\t\t" << std::fixed << std::setprecision(3)
       << it.second << "\n";
  }
  os << "Query time sum: " << total << "\n";
}

void TaskStatistics::printPerQueryToStream(std::ostream &os) const {
  using TimeRecord = std::tuple<std::size_t, double, double, std::string, std::string>;
  std::vector<TimeRecord> time_records;

  scheduler_.forEachTaskEvent([&](const auto &task_event) {
    const TaskDescription &description = std::get<0>(task_event);
    if (description.getTaskType() == TaskType::kRelationalOperator) {
      const double start =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::get<2>(task_event)).count() / 1000000000.0;
      const double end =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::get<3>(task_event)).count() / 1000000000.0;

      const auto token = description.getTaskMajorId();
      const std::string name = "q" + std::to_string(token);

      const auto *generator = description.getTaskName();
      const std::string subname =
          generator == nullptr ? "" : generator->getName();

      time_records.emplace_back(std::get<1>(task_event),
                                start, end, name, subname);
    }
  });

  std::sort(time_records.begin(), time_records.end());

  for (const auto &it : time_records) {
    os << std::get<0>(it) << "\t"
       << std::fixed << std::setprecision(6)
       << std::get<1>(it) << "\t"
       << std::get<2>(it) << "\t"
       << std::get<3>(it) << "\t"
       << std::get<4>(it) << "\n";
  }
}

void TaskStatistics::summarizePerColumnPreprocessingToStream(std::ostream &os) const {
  std::map<std::string, double> time_slots;
  scheduler_.forEachTaskEvent([&](const auto &task_event) {
    const TaskDescription &description = std::get<0>(task_event);
    if (description.getTaskType() == TaskType::kPreprocessing) {
      const auto &start = std::get<2>(task_event);
      const auto &end = std::get<3>(task_event);

      const auto token = description.getTaskMajorId();
      const std::string name = "r" + std::to_string(token >> 16) + "." +
                               "c" + std::to_string(token & 0xffff);
      time_slots[name] += std::chrono::duration<double>(end - start).count();
    }
  });

  double total = 0;
  for (const auto &it : time_slots) {
    total += it.second;

    os << it.first << ":\t\t" << std::fixed << std::setprecision(3)
       << it.second << "\n";
  }
  os << "Column preprocessing time sum: " << total << "\n";
}

void TaskStatistics::printPerColumnPreprocessingToStream(std::ostream &os) const {
  using TimeRecord = std::tuple<std::size_t, double, double, std::string, std::string>;
  std::vector<TimeRecord> time_records;

  scheduler_.forEachTaskEvent([&](const auto &task_event) {
    const TaskDescription &description = std::get<0>(task_event);
    if (description.getTaskType() == TaskType::kPreprocessing) {
      const double start =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::get<2>(task_event)).count() / 1000000000.0;
      const double end =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::get<3>(task_event)).count() / 1000000000.0;

      const auto token = description.getTaskMajorId();
      const std::string name = "r" + std::to_string(token >> 16) + "." +
                               "c" + std::to_string(token & 0xffff);
      const auto *generator = description.getTaskName();
      const std::string subname =
          generator == nullptr ? "" : generator->getName();

      time_records.emplace_back(std::get<1>(task_event),
                                start, end, name, subname);
    }
  });

  std::sort(time_records.begin(), time_records.end());

  for (const auto &it : time_records) {
    os << std::get<0>(it) << "\t"
       << std::fixed << std::setprecision(6)
       << std::get<1>(it) << "\t"
       << std::get<2>(it) << "\t"
       << std::get<3>(it) << "\t"
       << std::get<4>(it) << "\n";
  }
}

}  // namespace hustle
