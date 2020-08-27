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

#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <cstdlib>

#include "operator_result.h"
#include "scheduler/task.h"
#include "utils/event_profiler.h"

namespace hustle::operators {

class Operator {
 public:
  virtual void execute(Task *ctx) = 0;

  std::size_t getOperatorIndex() const { return op_index_; }

  void setOperatorIndex(const std::size_t op_index) { op_index_ = op_index; }

  std::size_t getQueryId() const { return query_id_; }
  std::shared_ptr<OperatorResult> result_;

  // TODO(nicholas): Make private
  Task *createTask() {
    return CreateLambdaTask([this](Task *ctx) {
      ctx->setTaskType(TaskType::kRelationalOperator);
      ctx->setProfiling(true);
      ctx->setCascade(true);
      ctx->setTaskMajorId(query_id_);

      this->execute(ctx);
    });
  }

 protected:
  explicit Operator(const std::size_t query_id) : query_id_(query_id) {}

  const std::size_t query_id_;

 private:
  std::size_t op_index_;

  DISALLOW_COPY_AND_ASSIGN(Operator);
};
};  // namespace hustle::operators

#endif  // HUSTLE_OPERATOR_H
