#ifndef PROJECT_OPTIMIZER_EXECUTION_PLAN_HPP_
#define PROJECT_OPTIMIZER_EXECUTION_PLAN_HPP_

#include <cstddef>
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "scheduler/Task.hpp"
#include "utility/Macros.hpp"

namespace hustle {

class ExecutionPlan : public Task {
 public:
  explicit ExecutionPlan(const std::size_t query_id)
      : query_id_(query_id) {}

  void execute() override;

//  std::size_t addRelationalOperator(RelationalOperator *relational_operator) {
//    const std::size_t op_index = relational_operators_.size();
//    relational_operator->setOperatorIndex(op_index);
//    relational_operators_.emplace_back(relational_operator);
//    return op_index;
//  }
//
//  const RelationalOperator& getRelationalOperator(const std::size_t op_index) const {
//    return *relational_operators_[op_index];
//  }

  void createLink(const std::size_t producer_operator_index,
                  const std::size_t consumer_operator_index) {
    dependents_[producer_operator_index].emplace(consumer_operator_index);
  }

  const std::unordered_set<std::size_t>& getDependents(
      const std::size_t producer_operator_index) const {
    const auto it = dependents_.find(producer_operator_index);
    return it == dependents_.end() ? kEmptySet : it->second;
  }

//  std::size_t size() const {
//    return relational_operators_.size();
//  }

 private:
  const std::size_t query_id_;

//  std::vector<std::unique_ptr<RelationalOperator>> relational_operators_;
  std::unordered_map<std::size_t, std::unordered_set<std::size_t>> dependents_;

  static const std::unordered_set<std::size_t> kEmptySet;

  DISALLOW_COPY_AND_ASSIGN(ExecutionPlan);
};

}  // namespace project

#endif  // PROJECT_OPTIMIZER_EXECUTION_PLAN_HPP_
