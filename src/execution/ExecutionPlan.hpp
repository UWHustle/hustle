#ifndef HUSTLE_OPTIMIZER_EXECUTION_PLAN_HPP_
#define HUSTLE_OPTIMIZER_EXECUTION_PLAN_HPP_

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "operators/Operator.h"
#include "scheduler/Task.hpp"
#include "utils/Macros.hpp"

// TODO(nicholas): reorganize namespaces so you don't need to do this.
using hustle::operators::Operator;

namespace hustle {

class ExecutionPlan : public Task {
 public:
  explicit ExecutionPlan(const std::size_t query_id) : query_id_(query_id) {}

  void execute() override;

  std::size_t addOperator(Operator* op) {
    const std::size_t op_index = operators_.size();
    op->setOperatorIndex(op_index);
    operators_.emplace_back(op);
    return op_index;
  }

  const Operator& getOperator(const std::size_t op_index) const {
    return *operators_[op_index];
  }

  void createLink(const std::size_t producer_operator_index,
                  const std::size_t consumer_operator_index) {
    dependents_[producer_operator_index].emplace(consumer_operator_index);
  }

  const std::unordered_set<std::size_t>& getDependents(
      const std::size_t producer_operator_index) const {
    const auto it = dependents_.find(producer_operator_index);
    return it == dependents_.end() ? kEmptySet : it->second;
  }

  std::size_t size() const { return operators_.size(); }

 private:
  const std::size_t query_id_;

  std::vector<std::unique_ptr<Operator>> operators_;
  std::unordered_map<std::size_t, std::unordered_set<std::size_t>> dependents_;

  static const std::unordered_set<std::size_t> kEmptySet;

  DISALLOW_COPY_AND_ASSIGN(ExecutionPlan);
};

}  // namespace hustle

#endif  // PROJECT_OPTIMIZER_EXECUTION_PLAN_HPP_
