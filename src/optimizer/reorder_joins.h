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

#ifndef HUSTLE_OPTIMIZER_REORDER_JOIN_H
#define HUSTLE_OPTIMIZER_REORDER_JOIN_H

#include "execution/execution_plan.h"
#include "operators/select/predicate.h"
#include "operators/utils/operator_result.h"

namespace hustle {
namespace optimizer {
using namespace hustle::operators;

    typedef std::size_t GroupId;
    typedef std::size_t TableId;

    struct JoinInfo {
        TableId left_id;
        TableId right_id;
        std::size_t est_size;
        JoinInfo(TableId left_tbl, TableId right_tbl, std::size_t est_size)
                : left_id(left_tbl), right_id(right_tbl), est_size(est_size)
        {
        }
    };

class ReorderJoin {
 public:

    /**
     * Join ordering using GOO (Greedy Operator Ordering algorithm)
     * @param query_id
     * @param predicates Join predicates
     * @param input_result Input tables/prev result to the join operators
     * @return execution plan
     */
  static ExecutionPlan::PlanPtr ApplyJoinReordering(
          std::size_t query_id,
      std::vector<JoinPredicate> predicates,
      std::vector<OperatorResult::OpResultPtr> input_result);
};
}  // namespace optimizer

}  // namespace hustle

#endif  // HUSTLE_OPTIMIZER_REORDER_JOIN_H