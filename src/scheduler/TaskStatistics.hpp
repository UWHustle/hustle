#ifndef PROJECT_SCHEDULER_TASK_STATISTICS_HPP_
#define PROJECT_SCHEDULER_TASK_STATISTICS_HPP_

#include <ostream>

#include "scheduler/Scheduler.hpp"
#include "utils/Macros.hpp"

namespace hustle {

class TaskStatistics {
 public:
  explicit TaskStatistics(const Scheduler &scheduler) : scheduler_(scheduler) {}

  void summarizePerQueryToStream(std::ostream &os) const;
  void printPerQueryToStream(std::ostream &os) const;

  void printPerColumnPreprocessingToStream(std::ostream &os) const;
  void summarizePerColumnPreprocessingToStream(std::ostream &os) const;

 private:
  // We might want to construct some indexing data structures ...
  const Scheduler &scheduler_;

  DISALLOW_COPY_AND_ASSIGN(TaskStatistics);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_TASK_STATISTICS_HPP_
