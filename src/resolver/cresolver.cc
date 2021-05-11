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

#include "resolver/cresolver.h"

#include <iostream>
#include <optional>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "execution/execution_plan.h"
#include "operators/aggregate/hash_aggregate.h"
#include "operators/fused/filter_join.h"
#include "operators/fused/select_build_hash.h"
#include "operators/join/hash_join.h"
#include "operators/select/predicate.h"
#include "operators/select/select.h"
#include "operators/utils/operator_result.h"
#include "resolver/select_resolver.h"
#include "scheduler/threading/synchronization_lock.h"
#include "storage/table.h"
#include "utils/event_profiler.h"

#define OP_PERF_ANALYSIS 1

#define ENABLE_FUSED_OPERATOR 1
#define NULL_OP_ID -1
#define DEFAULT_QUERY_ID 0

using namespace hustle::operators;
using namespace hustle::resolver;

using SelectPtr = std::unique_ptr<hustle::operators::Select>;
using AggPtr = std::unique_ptr<HashAggregate>;
using JoinPtr = std::unique_ptr<MultiwayJoin>;
using HashJoinPtr = std::unique_ptr<HashJoin>;
using FilterJoinPtr = std::unique_ptr<FilterJoin>;
using ProjectReferencePtr = std::shared_ptr<ProjectReference>;

std::optional<bool> build_select(
    Catalog *catalog,
    std::shared_ptr<hustle::resolver::SelectResolver> select_resolver,
    std::unordered_map<std::string, std::shared_ptr<PredicateTree>>
        select_predicates,
    std::unordered_map<std::string, OperatorResult::OpResultPtr>
        &select_result_map,
    std::vector<OperatorResult::OpResultPtr> &select_result,
    std::vector<SelectPtr> &select_operators) {
  /**
   * Iterate the through select predicates and src tables
   * to construct the select operators.
   */
  bool is_predicate_avail = false;
  auto join_predicate_map = select_resolver->join_predicates();
  for (auto const &[table_name, predicate_tree] : select_predicates) {
    OperatorResult::OpResultPtr input_result =
        std::make_shared<OperatorResult>();
    OperatorResult::OpResultPtr output_result =
        std::make_shared<OperatorResult>();
    auto table_ptr = catalog->GetTable(table_name);
    if (table_ptr == nullptr) return std::nullopt;
    std::unique_ptr<hustle::operators::Select> select;
    if (ENABLE_FUSED_OPERATOR &&
        join_predicate_map.find(table_name) != join_predicate_map.end() &&
        join_predicate_map.size() > 1) {
      is_predicate_avail = true;
      input_result->append(table_ptr);
      select = std::make_unique<hustle::operators::SelectBuildHash>(
          0, table_ptr, input_result, output_result, predicate_tree,
          join_predicate_map[table_name].right_col_);
      select_operators.emplace_back(std::move(select));
    } else if (predicate_tree == nullptr) {
      output_result->append(table_ptr);
    } else {
      is_predicate_avail = true;
      input_result->append(table_ptr);

      if ((!ENABLE_FUSED_OPERATOR ||
           join_predicate_map.find(table_name) == join_predicate_map.end()) &&
          (join_predicate_map.size() <= 1)) {
        select = std::make_unique<hustle::operators::Select>(
            DEFAULT_QUERY_ID, table_ptr, input_result, output_result,
            predicate_tree);
      } else {
        select = std::make_unique<hustle::operators::SelectBuildHash>(
            0, table_ptr, input_result, output_result, predicate_tree,
            join_predicate_map[table_name].right_col_);
      }
      select_operators.emplace_back(std::move(select));
    }
    select_result_map[table_name] = output_result;
    select_result.emplace_back(output_result);
  }
  return is_predicate_avail;
}

void build_linear_join(
    const std::vector<JoinPredicate> &predicates,
    std::unordered_map<std::string, std::shared_ptr<PredicateTree>>
        select_predicates,
    std::vector<HashJoinPtr> &join_ptrs,
    std::unordered_map<std::string, OperatorResult::OpResultPtr>
        &select_result_map,
    std::vector<OperatorResult::OpResultPtr> &select_result,
    OperatorResult::OpResultPtr &join_result_out) {
  std::shared_ptr<std::vector<OperatorResult::OpResultPtr>> input_result;
  OperatorResult::OpResultPtr prev_join_result, output_result;
  bool is_first_join = true;
  for (auto join_predicate : predicates) {
    OperatorResult::OpResultPtr left_result = nullptr, right_result = nullptr;
    if (!is_first_join) {
      left_result = prev_join_result;
    }
    if (right_result == nullptr) {
      auto predicate_tree_itr =
          select_predicates.find(join_predicate.right_col_.table->get_name());
      if (predicate_tree_itr != select_predicates.end()) {
        right_result =
            select_result_map[join_predicate.right_col_.table->get_name()];
      } else {
        right_result = std::make_shared<OperatorResult>();
        right_result->append(join_predicate.right_col_.table);
      }
    }

    if (left_result == nullptr) {
      auto predicate_tree_itr =
          select_predicates.find(join_predicate.left_col_.table->get_name());
      if (predicate_tree_itr != select_predicates.end()) {
        left_result =
            select_result_map[join_predicate.left_col_.table->get_name()];
      } else {
        left_result = std::make_shared<OperatorResult>();
        left_result->append(join_predicate.left_col_.table);
      }
    }

    input_result = std::make_shared<std::vector<OperatorResult::OpResultPtr>>();
    input_result->assign({left_result, right_result});
    output_result = std::make_shared<OperatorResult>();
    join_ptrs.emplace_back(std::make_unique<HashJoin>(
        0, input_result, output_result,
        std::make_shared<JoinPredicate>(join_predicate)));
    prev_join_result = output_result;
    is_first_join = false;
  }
  join_result_out = prev_join_result;
}

void build_join(
    JoinType join_type,
    std::unordered_map<std::string, JoinPredicate> &join_predicate_map,
    bool is_predicate_avail, JoinPtr &join_op, FilterJoinPtr &filter_join_op,
    std::vector<OperatorResult::OpResultPtr> &select_result,
    OperatorResult::OpResultPtr &join_result_out) {
  std::vector<JoinPredicate> join_predicates(join_predicate_map.size());
  std::transform(join_predicate_map.begin(), join_predicate_map.end(),
                 join_predicates.begin(),
                 [](auto &pred) { return pred.second; });
  JoinGraph join_graph({join_predicates});
  join_result_out = std::make_shared<OperatorResult>();
  if (ENABLE_FUSED_OPERATOR && join_graph.num_predicates() > 1 &&
      is_predicate_avail) {
    filter_join_op = std::make_unique<FilterJoin>(0, select_result,
                                                  join_result_out, join_graph);
  } else {
    join_op = std::make_unique<MultiwayJoin>(DEFAULT_QUERY_ID, select_result,
                                             join_result_out, join_graph);
  }
}

void build_aggregate(
    std::shared_ptr<hustle::resolver::SelectResolver> select_resolver,
    std::shared_ptr<std::vector<AggregateReference>> &agg_refs, AggPtr &agg_op,
    OperatorResult::OpResultPtr &prev_result_out,
    OperatorResult::OpResultPtr &agg_result_out) {
  /**
   * Group by references and order by references from select resolver
   */
  auto group_by_ref_ptrs = *(select_resolver->groupby_references());
  std::vector<ColumnReference> group_by_refs(group_by_ref_ptrs.size());
  std::transform(
      group_by_ref_ptrs.begin(), group_by_ref_ptrs.end(), group_by_refs.begin(),
      [](std::shared_ptr<hustle::operators::ColumnReference> x) { return *x; });

  auto order_by_ref_ptrs = *(select_resolver->orderby_references());
  std::vector<OrderByReference> order_by_refs(order_by_ref_ptrs.size());
  std::transform(order_by_ref_ptrs.begin(), order_by_ref_ptrs.end(),
                 order_by_refs.begin(),
                 [](std::shared_ptr<hustle::operators::OrderByReference> x) {
                   return *x;
                 });

  agg_op = std::make_unique<HashAggregate>(DEFAULT_QUERY_ID, prev_result_out,
                                           agg_result_out, *agg_refs,
                                           group_by_refs, order_by_refs);
}

void build_output_cols(bool is_agg_out,
                       std::vector<ProjectReferencePtr> &project_references,
                       std::vector<ColumnReference> &agg_project_cols) {
  for (auto project_ref : project_references) {
    auto table_ptr = is_agg_out ? nullptr : project_ref->colRef.table;
    if (!project_ref->alias.empty()) {
      agg_project_cols.emplace_back(
          ColumnReference{table_ptr, project_ref->alias});
    } else {
      agg_project_cols.emplace_back(
          ColumnReference{table_ptr, project_ref->colRef.col_name});
    }
  }
}

std::shared_ptr<hustle::ExecutionPlan> createPlan(
    std::shared_ptr<hustle::resolver::SelectResolver> select_resolver,
    Catalog *catalog) {
  using namespace hustle::operators;
  std::unordered_map<std::string, std::shared_ptr<PredicateTree>>
      select_predicates = select_resolver->select_predicates();
  std::vector<OperatorResult::OpResultPtr> select_result;
  std::unordered_map<std::string, OperatorResult::OpResultPtr>
      select_result_map;
  std::vector<SelectPtr> select_operators;
  auto join_predicate_map = select_resolver->join_predicates();

  std::optional<bool> is_predicate_avail =
      build_select(catalog, select_resolver, select_predicates,
                   select_result_map, select_result, select_operators);
  if (is_predicate_avail == std::nullopt) return nullptr;

  /**
   * Get the join predicates and the previous results to construct
   * the join operators.
   */
  OperatorResult::OpResultPtr join_result_out;
  JoinPtr join_op = nullptr;
  FilterJoinPtr filter_join_op = nullptr;
  std::vector<HashJoinPtr> hash_join_ptrs;

  if (join_predicate_map.size() != 0) {
    if (select_resolver->join_type() == JoinType::STAR) {
      build_join(select_resolver->join_type(), join_predicate_map,
                 is_predicate_avail.value(), join_op, filter_join_op,
                 select_result, join_result_out);
    } else if (select_resolver->join_type() == JoinType::LINEAR) {
      build_linear_join(select_resolver->predicates(),
                        select_resolver->select_predicates(), hash_join_ptrs,
                        select_result_map, select_result, join_result_out);
    }
  } else {
    join_result_out = select_result[0];
  }

  std::shared_ptr<std::vector<AggregateReference>> agg_refs =
      (select_resolver->agg_references());

  OperatorResult::OpResultPtr agg_result_out =
      std::make_shared<OperatorResult>();

  AggPtr agg_op = nullptr;
  if (agg_refs->size() != 0) {
    assert(agg_refs->size() == 1);  // currently supports one agg op
    build_aggregate(select_resolver, agg_refs, agg_op, join_result_out,
                    agg_result_out);
  } else {
    agg_result_out = join_result_out;
  }

  // Build the output columns for the result
  std::vector<ProjectReferencePtr> project_references =
      *(select_resolver->project_references());
  std::vector<ColumnReference> agg_project_cols;
  build_output_cols(agg_op != nullptr, project_references, agg_project_cols);

  std::shared_ptr<hustle::ExecutionPlan> plan =
      std::make_shared<hustle::ExecutionPlan>(0);

  size_t join_id = NULL_OP_ID, agg_id = NULL_OP_ID, select_id = NULL_OP_ID;
  if (agg_op != nullptr) {
    agg_id = plan->addOperator(std::move(agg_op));
    plan->setOperatorResult(agg_result_out);
  }

  std::vector<std::size_t> join_ids;
  if (join_op != nullptr) {
    join_id = plan->addOperator(std::move(join_op));
  } else if (filter_join_op != nullptr) {
    join_id = plan->addOperator(std::move(filter_join_op));
  } else if (hash_join_ptrs.size() > 0) {
    for (int i = 0; i < hash_join_ptrs.size(); i++) {
      join_ids.emplace_back(plan->addOperator(std::move(hash_join_ptrs.at(i))));
    }
  }
  if (join_id != NULL_OP_ID && plan->getOperatorResult() == nullptr) {
    plan->setOperatorResult(join_result_out);
  }

  for (auto &select_op : select_operators) {
    select_id = plan->addOperator(std::move(select_op));
    if (join_id != NULL_OP_ID) {
      plan->createLink(select_id, join_id);
    } else if (!join_ids.empty()) {
      plan->createLink(select_id, join_ids.at(0));
    }
  }

  if (!join_ids.empty()) {
    for (int i = 0; i < join_ids.size() - 1; i++) {
      plan->createLink(join_ids.at(i), join_ids.at(i + 1));
    }
  }
  if (select_id != NULL_OP_ID && plan->getOperatorResult() == nullptr) {
    // Since you do not have a join there will be only one table
    assert(select_result.size() == 1);
    plan->setOperatorResult(select_result.at(0));
  }

  // Declare aggregate dependency on join operator
  if (agg_id != NULL_OP_ID) {
    if (join_id == NULL_OP_ID && join_ids.empty()) {
      if (select_operators.size() != 1) return nullptr;
      plan->createLink(select_id, agg_id);
    } else {
      if (!join_ids.empty()) {
        plan->createLink(join_ids.at(join_ids.size() - 1), agg_id);
      } else {
        plan->createLink(join_id, agg_id);
      }
    }
    plan->setOperatorResult(agg_result_out);
  }
  plan->setResultColumns(agg_project_cols);
  return plan;
}

std::shared_ptr<hustle::storage::DBTable> execute(
    std::shared_ptr<hustle::ExecutionPlan> plan) {
  std::shared_ptr<hustle::storage::DBTable> out_table;
  using namespace hustle::operators;
  hustle::Scheduler &scheduler = hustle::HustleDB::get_scheduler();
  //std::cout << "Execute in Hustle" << std::endl;
  SynchronizationLock sync_lock;
  scheduler.addTask(CreateTaskChain(
      hustle::CreateLambdaTask([&plan](hustle::Task *ctx) {
        ctx->spawnLambdaTask([&plan](hustle::Task *internal) {
          if (plan->size() != 0) {
            internal->spawnTask(plan.get());
          }
        });
      }),
      hustle::CreateLambdaTask([&plan, &out_table, &sync_lock] {
        OperatorResult::OpResultPtr agg_result_out = plan->getOperatorResult();
        out_table = agg_result_out->materialize(plan->getResultColumns());
        sync_lock.release();
      })));
  sync_lock.wait();

  return out_table;
}

int resolveSelect(char *dbName, HustleMemLog *mem_log, Sqlite3Select *queryTree, void *pArgs,
                  sqlite3_callback xCallback) {
  using hustle::resolver::SelectResolver;
  auto catalog = hustle::HustleDB::get_catalog(dbName);
  if (dbName == NULL || catalog == nullptr) return 0;

  auto select_resolver = std::make_shared<SelectResolver>(catalog);
  bool is_resolvable =  false;
  if (OP_PERF_ANALYSIS) {
      auto container = hustle::profiler.getContainer();
      container->startEvent("resolver");
      is_resolvable = select_resolver->ResolveSelectTree(queryTree);
      container->endEvent("resolver");
  } else {
      is_resolvable = select_resolver->ResolveSelectTree(queryTree);
  }
  //std::cout << "Is resolvable: " << is_resolvable << std::endl;
  if (is_resolvable) {
      if (MEMLOG_LAZY_UPDATE) {
          auto container = hustle::profiler.getContainer();
          int query_type = DBTable::query_type;
          container->startEvent("lazy_update" + std::to_string(query_type));
          char** table_name_array = (char**)malloc(select_resolver->source_tables().size() * sizeof(char*));
          auto source_tables = select_resolver->source_tables();
          for(std::size_t i = 0; i < source_tables.size(); i++) {
              table_name_array[i] = source_tables[i].data();
          }
          hustle_memlog_table_update_db(mem_log, table_name_array, source_tables.size(), MEMLOG_UPDATE_FREE);
          free(table_name_array);
          // std::cout << "Query type:: " <<query_type << std::endl;
          container->endEvent("lazy_update" + std::to_string(query_type));
          //hustle_memlog_clear(mem_log);
      }
    std::shared_ptr<hustle::ExecutionPlan> plan =
        createPlan(select_resolver, catalog.get());
    if (plan != nullptr) {
      std::shared_ptr<hustle::storage::DBTable> outTable = execute(plan);
      outTable->out_table(pArgs, xCallback);
        if (OP_PERF_ANALYSIS) {
          //  hustle::profiler.summarizeToStream(std::cout);
        }
    } else {
        if (OP_PERF_ANALYSIS) {
           // hustle::profiler.summarizeToStream(std::cout);
        }
      return 0;
    }
  }
    if (OP_PERF_ANALYSIS) {
        //hustle::profiler.summarizeToStream(std::cout);
    }
  return is_resolvable ? 1 : 0;
}
