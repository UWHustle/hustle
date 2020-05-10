#include "optimizer/ExecutionPlan.hpp"

#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "scheduler/SchedulerTypedefs.hpp"
#include "scheduler/SchedulerInterface.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/SyncStream.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace hustle {

DEFINE_bool(print_per_query_span, false, "Print per-query time span");

const std::unordered_set<std::size_t> ExecutionPlan::kEmptySet;

void ExecutionPlan::execute() {
  SchedulerInterface *scheduler = getScheduler();

  const Continuation c_enter = scheduler->allocateContinuation();
  const Continuation c_exit = scheduler->allocateContinuation();

  std::vector<TaskID> task_ids(operators_.size());
  std::vector<Continuation> c_operators(operators_.size());

  for (std::unique_ptr<Operator> &op : operators_) {
    DCHECK(op != nullptr);
    const std::size_t op_index = op->getOperatorIndex();
    const Continuation op_cont = scheduler->allocateContinuation();

    const TaskID op_task_id =
        scheduler->addTask(op->createTask(), c_enter, op_cont);
    scheduler->addLink(op_cont, c_exit);

    task_ids[op_index] = op_task_id;
    c_operators[op_index] = op_cont;
  }

  // No inter-operator pipelining, though ...
  for (const auto &producer_it : dependents_) {
    const Continuation c_producer = c_operators[producer_it.first];
    for (const std::size_t consumer_op_index : producer_it.second) {
      scheduler->addLink(c_producer, task_ids[consumer_op_index]);
    }
  }

  // Postpone destruction of relational operators.
  auto operators =
      std::make_shared<decltype(operators_)>(
          std::move(operators_));

  auto start_time = std::make_shared<clock::time_point>(clock::now());

  scheduler->addTask(
      CreateLambdaTask([qid = query_id_, operators, start_time]{
        if (FLAGS_print_per_query_span) {
          const auto &zero_time = simple_profiler.zero_time();
          const double start = std::chrono::duration<double>(
              *start_time - zero_time).count();
          const double end = std::chrono::duration<double>(
              clock::now() - zero_time).count();

          SyncStream(std::cerr)
              << "[Q-" << qid << "](" << start << ", " << end << ") ";
        }
      }),
      c_exit, getContinuation());

  scheduler->fire(c_enter);
}

}  // namespace project
