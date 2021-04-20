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

#include "operators/select/predicate.h"
#include "operators/utils/operator_options.h"
#include "operators/utils/operator_result.h"
#include "scheduler/task.h"
#include "utils/config.h"
#include "utils/event_profiler.h"
#include <unordered_map>

#define OP_PERF_ANALYSIS 0

namespace hustle::operators {

    enum OperatorType
    {
        SELECT,
        HASH_JOIN,
        MULTI_JOIN, // i removed the rest since it's sequential
        HASH_BASED_AGGREGATE,
        AGGREGATE, // changed spelling
        LIP_OP,
        FILTER_JOIN,
        SELECT_BUILD_HASH,
    };


    using OperatorName = std::string;
    static const std::unordered_map<int, OperatorName> operator_names = {
            { OperatorType::SELECT, "Select" },
            { OperatorType::HASH_JOIN, "Hash Join" },
            { OperatorType::MULTI_JOIN, "Multi Join" },
            { OperatorType::HASH_BASED_AGGREGATE, "Hash Aggregate" },
            { OperatorType::AGGREGATE, "Aggregate" },
            { OperatorType::LIP_OP, "LIP" },
            { OperatorType::FILTER_JOIN, "Filter Join" },
            { OperatorType::SELECT_BUILD_HASH, "Select Build Hash" },
    };






    class Operator {
 public:

     void execute(Task *ctx) {
         if (OP_PERF_ANALYSIS) {
             auto container = profiler.getContainer();
             container->startEvent(this->operator_name() + " " + std::to_string(this->operator_index()));
             this->Execute(ctx, 0);
             container->endEvent(this->operator_name() + " " + std::to_string(this->operator_index()));
             profiler.summarizeToStream(std::cout);
         } else {
             this->Execute(ctx, 0);
         }
         //profiler.clear();
     }

    virtual void Execute(Task *ctx, int32_t flags) = 0;

    virtual void Clear() = 0;

    virtual std::string operator_name() = 0;

    inline std::size_t operator_index() const { return op_index_; }

  inline void set_operator_index(const std::size_t op_index) {
    op_index_ = op_index;
  }

  inline void set_query_id(std::size_t query_id) { query_id_ = query_id; }

  inline void set_operator_options(std::shared_ptr<OperatorOptions> options) {
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

  OperatorResult::OpResultPtr result_;

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
