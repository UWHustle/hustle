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

#include "optimizer/reorder_joins.h"

#include <map>
#include <memory>
#include <queue>
#include <vector>

#include "operators/join/hash_join.h"
#include "utils/disjoint_set_forest.h"

namespace hustle {
namespace optimizer {

using namespace hustle::operators;

using OperatorResultVec = std::vector<OperatorResult::OpResultPtr>;
using TableIdMap = std::unordered_map<std::shared_ptr<HustleTable>, std::size_t>;
using TablePtrMap = std::unordered_map<std::size_t, std::shared_ptr<HustleTable>>;

ExecutionPlan::PlanPtr ReorderJoin::ApplyJoinReordering(
    const std::size_t query_id, const std::vector<JoinPredicate> predicates,
    const OperatorResultVec input_result) {
  TableIdMap table_ids;
  TablePtrMap table_ptrs;

  std::vector<std::size_t> sizes;

  auto populate_tables_maps = [&](std::shared_ptr<HustleTable> tablePtr) {
    if (table_ids.find(tablePtr) == table_ids.end()) {
      table_ids[tablePtr] = table_ids.size();
      table_ptrs[table_ids[tablePtr]] = tablePtr;
    }
  };

  for (auto join_predicate : predicates) {
    populate_tables_maps(join_predicate.left_col_.table);
    populate_tables_maps(join_predicate.right_col_.table);
  }

  DCHECK(table_ids.size() >= 2);
  sizes.resize(table_ids.size());

  // populate table sizes
  for (auto const& [table, table_id] : table_ids) {
    sizes[table_id] = table->get_num_rows();
  }

  auto cmp_join = [](const JoinInfo& left, const JoinInfo& right) {
    return left.est_size > right.est_size;
  };
  std::priority_queue<JoinInfo, std::vector<JoinInfo>, decltype(cmp_join)>
      min_join_q;
  std::vector<ExecutionPlan::PlanPtr> plans(table_ids.size());
  DisjointSetForest<GroupId> join_forest;

  // TODO (suryadev): Use selectivity in the cost function
  auto compute_cost = [&](const TableId left_id, const TableId right_id) {
    return sizes[left_id] * sizes[right_id];
  };

  for (auto join_predicate : predicates) {
    TableId left_id = table_ids[join_predicate.left_col_.table];
    TableId right_id = table_ids[join_predicate.right_col_.table];
    join_forest.makeSet(left_id);
    join_forest.makeSet(right_id);
    min_join_q.push(
        JoinInfo{left_id, right_id,
                 std::make_shared<decltype(join_predicate)>(join_predicate),
                 compute_cost(left_id, right_id)});
  }

  std::size_t final_plan_id = -1;

  // Use greedy approach to find the join order
  while (!min_join_q.empty()) {
    JoinInfo min_join_info = min_join_q.top();
    min_join_q.pop();

    const std::size_t lgrp_id = join_forest.find(min_join_info.left_id);
    const std::size_t rgrp_id = join_forest.find(min_join_info.right_id);
    if (lgrp_id == rgrp_id) {
      // Currently, join with cycles are not supported
      throw std::runtime_error("Unsupported join query");
    }

    auto join_out = std::make_shared<OperatorResult>();
    OperatorResultVec input_vec;

    auto input_result = [&](OperatorResultVec& input_vec, const int tbl_id,
                            const GroupId grp_id) {
      if (plans[grp_id] == nullptr) {
        auto out_result = std::make_shared<OperatorResult>();
        out_result->append(table_ptrs[tbl_id]);
        input_vec.emplace_back(out_result);
        plans[grp_id] = std::make_shared<hustle::ExecutionPlan>(query_id);
      } else {
        input_vec.emplace_back(plans[grp_id]->getOperatorResult());
      }
    };

    input_result(input_vec, min_join_info.left_id, lgrp_id);
    input_result(input_vec, min_join_info.right_id, rgrp_id);

    // Merge both the plans by adding the join
    plans[rgrp_id]->addOperator(std::move(std::make_unique<HashJoin>(
        query_id, input_vec, join_out, min_join_info.pred)));

    plans[lgrp_id]->merge(*plans[rgrp_id]);
    plans[lgrp_id]->setOperatorResult(join_out);
    plans[rgrp_id].reset();

    join_forest.merge(min_join_info.left_id, min_join_info.right_id);

    const std::size_t merged_id = join_forest.find(min_join_info.left_id);
    plans[merged_id] = plans[lgrp_id];

    final_plan_id = merged_id;
  }
  if (final_plan_id == -1) return nullptr;
  ExecutionPlan::PlanPtr result_plan = plans[final_plan_id];

  return result_plan;
}

}  // namespace optimizer
}  // namespace hustle
