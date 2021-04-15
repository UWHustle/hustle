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

#ifndef HUSTLE_SSB_WORKLOAD_H
#define HUSTLE_SSB_WORKLOAD_H

#include <operators/aggregate/aggregate_const.h>

#include "execution/execution_plan.h"
#include "operators/select/predicate.h"
#include "operators/utils/aggregate_factory.h"
#include "operators/utils/operator_options.h"
#include "scheduler/scheduler.h"
#include "storage/base_table.h"

namespace hustle::operators {
class SSB {
 public:
  explicit SSB(int SF, bool print, hustle::operators::AggregateType agg_type);

  void execute(ExecutionPlan &plan,
               OperatorResult::OpResultPtr &final_result);


  void q11();
  void q12();
  void q13();

  void q21();
  void q22();
  void q23();

  void q31();
  void q32();
  void q33();
  void q34();

  void q41();
  void q42();
  void q43();

  void q11_lip();
  void q12_lip();
  void q13_lip();

  void q21_lip();
  void q22_lip();
  void q23_lip();

  void q31_lip();
  void q32_lip();
  void q33_lip();
  void q34_lip();

  void q41_lip();
  void q42_lip();
  void q43_lip();

  std::shared_ptr<HustleTable> lo, c, s, p, d;
  std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema,
      d_schema;

 private:
  bool print_;
  int num_threads_;
  AggregateType aggregate_type;
  std::shared_ptr<Scheduler> scheduler;

  OperatorResult::OpResultPtr lo_result_in;
  OperatorResult::OpResultPtr d_result_in;
  OperatorResult::OpResultPtr p_result_in;
  OperatorResult::OpResultPtr s_result_in;
  OperatorResult::OpResultPtr c_result_in;

  OperatorResult::OpResultPtr lo_select_result_out;
  OperatorResult::OpResultPtr d_select_result_out;
  OperatorResult::OpResultPtr p_select_result_out;
  OperatorResult::OpResultPtr s_select_result_out;
  OperatorResult::OpResultPtr c_select_result_out;

  std::vector<std::shared_ptr<OperatorResult>> lip_result_in;
  std::vector<std::shared_ptr<OperatorResult>> join_result_in;

  OperatorResult::OpResultPtr lip_result_out;
  OperatorResult::OpResultPtr join_result_out;
  OperatorResult::OpResultPtr agg_result_out;

  std::shared_ptr<OperatorOptions> select_options;
  std::shared_ptr<OperatorOptions> join_options;
  std::shared_ptr<OperatorOptions> filter_join_options;
  std::shared_ptr<OperatorOptions> aggregate_options;

  std::shared_ptr<HustleTable> out_table;

  ColumnReference lo_d_ref;
  ColumnReference lo_p_ref;
  ColumnReference lo_s_ref;
  ColumnReference lo_c_ref;

  ColumnReference d_ref;
  ColumnReference p_ref;
  ColumnReference s_ref;
  ColumnReference c_ref;

  JoinPredicate d_join_pred;
  JoinPredicate p_join_pred;
  JoinPredicate s_join_pred;
  JoinPredicate c_join_pred;

  ColumnReference lo_rev_ref;
  ColumnReference d_year_ref;
  ColumnReference p_brand1_ref;
  ColumnReference p_category_ref;
  ColumnReference s_nation_ref;
  ColumnReference s_city_ref;
  ColumnReference c_nation_ref;
  ColumnReference c_city_ref;

  void reset_results();
};

}  // namespace hustle::operators
#endif  // HUSTLE_SSB_WORKLOAD_H
