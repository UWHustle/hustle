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
using JoinPredicateVec = std::vector<std::shared_ptr<JoinPredicate>>;
using TableIdMap = std::unordered_map<DBTable::TablePtr, std::size_t>;
using TablePtrMap = std::unordered_map<std::size_t, DBTable::TablePtr>;

ExecutionPlan::PlanPtr ReorderJoin::ApplyJoinReordering(
    std::size_t query_id, std::vector<JoinPredicate> predicates,
    OperatorResultVec input_result) {
  TableIdMap table_id_map;
  TablePtrMap table_ptr_map;

  std::vector<std::size_t> sizes;
  std::vector<JoinPredicateVec> join_predicates_tbl;

  auto populate_tables_maps = [&](DBTable::TablePtr tablePtr) {
    if (table_id_map.find(tablePtr) == table_id_map.end()) {
      table_id_map[tablePtr] = table_id_map.size();
      std::cout << "size: " << table_id_map.size() << std::endl;
      std::cout << tablePtr->get_name() << std::endl;
      table_ptr_map[table_id_map[tablePtr]] = tablePtr;
    }
  };

  for (auto join_predicate : predicates) {
    std::cout << "Join Predicate" << std::endl;
    populate_tables_maps(join_predicate.left_col_.table);
    populate_tables_maps(join_predicate.right_col_.table);
  }

  DCHECK(table_id_map.size() >= 2);

  sizes.resize(table_id_map.size());
  join_predicates_tbl.resize(table_id_map.size());

  // populate join predicates table
  for (std::size_t i = 0; i < table_id_map.size(); i++) {
    JoinPredicateVec join_predicates;
    join_predicates.resize(table_id_map.size());
    join_predicates_tbl[i] = join_predicates;
  }

  // populate table sizes
  for (auto const& [table, table_id] : table_id_map) {
    sizes[table_id] = table->get_num_rows();
  }

  auto cmp_join = [](JoinInfo& left, JoinInfo& right) {
    return left.est_size > right.est_size;
  };
  std::priority_queue<JoinInfo, std::vector<JoinInfo>, decltype(cmp_join)>
      min_join_q;

  auto compute_cost = [&](TableId l_id, TableId r_id) {
    return sizes[l_id] * sizes[r_id];
  };
  DisjointSetForest<GroupId> join_table_forest;
  for (auto join_predicate : predicates) {
    TableId left_tbl_id = table_id_map[join_predicate.left_col_.table];
    TableId right_tbl_id = table_id_map[join_predicate.right_col_.table];

    join_predicates_tbl[left_tbl_id][right_tbl_id] =
        join_predicates_tbl[right_tbl_id][left_tbl_id] =
            std::make_shared<decltype(join_predicate)>(join_predicate);
    join_table_forest.makeSet(left_tbl_id);
    join_table_forest.makeSet(right_tbl_id);
    min_join_q.push(
        {left_tbl_id, right_tbl_id, compute_cost(left_tbl_id, right_tbl_id)});
  }

  std::vector<ExecutionPlan::PlanPtr> plans(table_id_map.size());

  for (std::size_t table_idx = 0; table_idx < table_id_map.size();
       table_idx++) {
    plans[table_idx] = std::make_shared<hustle::ExecutionPlan>(query_id);
  }

  std::size_t final_plan_group_id = -1;
  std::cout << "Size: " << table_id_map.size() << std::endl;

  while (true) {
    int min_size = -1;
    int min_lidx = -1;
    int min_ridx = -1;
    std::shared_ptr<JoinPredicate> min_predicate;

    // Greedily find the join with minimum cost
    for (int l_idx = table_id_map.size() - 1; l_idx >= 0; l_idx--) {
      for (int r_idx = table_id_map.size() - 1; r_idx >= 0 && l_idx != r_idx;
           r_idx--) {
        // TODO (srsuryadev): Replace the simple cost with selectivity parameter
        if (join_predicates_tbl[l_idx][r_idx] != nullptr) {
          auto estimated_cost = sizes[l_idx] * sizes[r_idx];
          if (min_size == -1) min_size = estimated_cost;
          if (estimated_cost <= min_size) {
            min_predicate = join_predicates_tbl[l_idx][r_idx];
            min_lidx = l_idx;
            min_ridx = r_idx;
          }
        }
      }
    }

    if (min_lidx == -1 && min_ridx == -1) {
      break;
    }

    DCHECK(min_lidx != -1 && min_ridx != -1);

    auto lgroup_id = join_table_forest.find(min_lidx);
    auto rgroup_id = join_table_forest.find(min_ridx);
    if (plans[rgroup_id] == nullptr || lgroup_id == rgroup_id) {
      // Currently, join with cycles are not supported
      throw std::runtime_error("Unsupported join query");
    }

    ExecutionPlan::PlanPtr lplan = plans[lgroup_id], rplan = plans[rgroup_id];

    auto join_result_out = std::make_shared<OperatorResult>();
    OperatorResultVec input_vec;

    auto get_input_result = [&](OperatorResultVec& result_vec, int table_id,
                                GroupId group_id) {
      if (plans[group_id]->size() == 0) {
        auto out_result = std::make_shared<OperatorResult>();
        out_result->append(table_ptr_map[table_id]);
        result_vec.emplace_back(out_result);
      } else {
        result_vec.emplace_back(plans[group_id]->getOperatorResult());
      }
    };

    get_input_result(input_vec, min_lidx, lgroup_id);
    get_input_result(input_vec, min_ridx, rgroup_id);
    plans[rgroup_id]->addOperator(std::move(std::make_unique<HashJoin>(
        query_id, input_vec, join_result_out, min_predicate)));
    plans[lgroup_id]->merge(*plans[rgroup_id]);
    plans[lgroup_id]->setOperatorResult(join_result_out);
    plans[rgroup_id] = std::make_shared<hustle::ExecutionPlan>(query_id);
    auto merged_plan = plans[lgroup_id];
    join_table_forest.merge(min_lidx, min_ridx);
    lgroup_id = join_table_forest.find(min_lidx);
    rgroup_id = join_table_forest.find(min_lidx);
    plans[lgroup_id] = merged_plan;

    final_plan_group_id = lgroup_id;
    std::cout << min_ridx << "  " << min_lidx << std::endl;
    join_predicates_tbl[min_ridx][min_lidx] =
        join_predicates_tbl[min_lidx][min_ridx] = nullptr;
  }
  std::cout << "out loop" << final_plan_group_id << std::endl;
  if (final_plan_group_id == -1) return nullptr;
  ExecutionPlan::PlanPtr result_plan = plans[final_plan_group_id];
  std::cout << "return" << final_plan_group_id << std::endl;

  return result_plan;
}

}  // namespace optimizer
}  // namespace hustle