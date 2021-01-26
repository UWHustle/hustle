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

#include "operators/operator_options.h"
#include "operators/predicate.h"
#include "operators/utils/operator_result.h"
#include "scheduler/task.h"
#include "utils/config.h"
#include "utils/event_profiler.h"

namespace hustle::operators {

class Operator {
 public:
  virtual void execute(Task *ctx) = 0;

  virtual void Clear() = 0;

  inline std::size_t operator_index() const { return op_index_; }

  inline void set_operator_index(const std::size_t op_index) { op_index_ = op_index; }

  inline void set_query_id(std::size_t query_id) {
    query_id_ = query_id;
  }

  inline void set_operator_options( std::shared_ptr<OperatorOptions> options) {
    options_ = options;
  }

  inline std::size_t query_id() const { return query_id_; }

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

  std::shared_ptr<OperatorResult> result_;

 protected:
  explicit Operator(std::size_t query_id) : query_id_(query_id) {
    options_ = std::make_shared<OperatorOptions>();
  }

  explicit Operator(const std::size_t query_id,
                    std::shared_ptr<OperatorOptions> options)
      : query_id_(query_id), options_(options) {}

  std::shared_ptr<OperatorOptions> options_;
  std::size_t query_id_;

 private:
  std::size_t op_index_;

  DISALLOW_COPY_AND_ASSIGN(Operator);
};
};  // namespace hustle::operators

#endif  // HUSTLE_OPERATOR_H
