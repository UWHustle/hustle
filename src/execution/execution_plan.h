// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_OPTIMIZER_EXECUTION_PLAN_HPP_
#define HUSTLE_OPTIMIZER_EXECUTION_PLAN_HPP_

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "operators/operator.h"
#include "scheduler/task.h"
#include "utils/macros.h"

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

  std::size_t addOperator(std::unique_ptr<Operator> op) {
    const std::size_t op_index = operators_.size();
    op->setOperatorIndex(op_index);
    operators_.emplace_back(std::move(op));
    return op_index;
  }

  void setOperatorResult(
      std::shared_ptr<operators::OperatorResult> operator_result) {
    result_out_ = operator_result;
  }

  void setResultColumns(std::vector<operators::ColumnReference> result_cols) {
    result_cols_ = result_cols;
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

  std::shared_ptr<operators::OperatorResult> getOperatorResult() const {
    return result_out_;
  }

  std::vector<operators::ColumnReference>& getResultColumns() {
    return result_cols_;
  }

  std::size_t size() const { return operators_.size(); }

  ~ExecutionPlan() override {}

 private:
  const std::size_t query_id_;

  std::vector<std::unique_ptr<Operator>> operators_;
  std::unordered_map<std::size_t, std::unordered_set<std::size_t>> dependents_;
  std::shared_ptr<operators::OperatorResult> result_out_;
  std::vector<operators::ColumnReference> result_cols_;
  static const std::unordered_set<std::size_t> kEmptySet;

  DISALLOW_COPY_AND_ASSIGN(ExecutionPlan);
};

}  // namespace hustle

#endif  // PROJECT_OPTIMIZER_EXECUTION_PLAN_HPP_
