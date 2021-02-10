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

#include "execution/execution_plan.h"
#include "operators/aggregate.h"
#include "operators/fused/filter_join.h"
#include "operators/fused/select_build_hash.h"
#include "operators/join.h"
#include "operators/join_graph.h"
#include "operators/lip.h"
#include "operators/predicate.h"
#include "operators/select.h"
#include "scheduler/scheduler.h"
#include "ssb_workload.h"
#include "storage/util.h"

namespace hustle::operators {

void SSB::q11_lip() {
  // discount >= 1
  auto discount_pred_1 =
      Predicate{{lo, "discount"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum((uint8_t)1)};
  auto discount_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_1));

  // discount <= 3
  auto discount_pred_2 = Predicate{{lo, "discount"},
                                   arrow::compute::CompareOperator::LESS_EQUAL,
                                   arrow::Datum((uint8_t)3)};
  auto discount_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_2));

  auto discount_connective_node = std::make_shared<ConnectiveNode>(
      discount_pred_node_1, discount_pred_node_2, FilterOperator::AND);

  // quantity < 25
  auto quantity_pred_1 = Predicate{{lo, "quantity"},
                                   arrow::compute::CompareOperator::LESS,
                                   arrow::Datum((uint8_t)25)};
  auto quantity_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(quantity_pred_1));

  auto lo_root_node = std::make_shared<ConnectiveNode>(
      quantity_pred_node_1, discount_connective_node, FilterOperator::AND);

  auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

  // date.year = 1993
  ColumnReference year_ref = d_year_ref;
  auto year_pred_1 = Predicate{{d, "year"},
                               arrow::compute::CompareOperator::EQUAL,
                               arrow::Datum((int64_t)1993)};
  auto year_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(year_pred_1));
  auto d_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  Select lo_select_op(0, lo, lo_result_in, lo_select_result_out, lo_pred_tree);
  Select d_select_op(0, d, d_result_in, d_select_result_out, d_pred_tree);

  lip_result_in = {lo_select_result_out, d_select_result_out};

  JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
  JoinGraph graph({{join_pred}});
  LIP lip_op(0, lip_result_in, lip_result_out, graph);
  Join join_op(0, {lip_result_out}, join_result_out, graph, join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {},
                   aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto lo_select_id = plan.addOperator(&lo_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);
  auto lip_id = plan.addOperator(&lip_op);
  auto join_id = plan.addOperator(&join_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(lo_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  plan.createLink(lip_id, join_id);

  // Declare aggregate dependency on join operator
  plan.createLink(join_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("1.1_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("1.1_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q12_lip() {
  // discount >= 4
  auto discount_pred_1 =
      Predicate{{lo, "discount"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum((uint8_t)4)};
  auto discount_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_1));

  // discount <= 6
  auto discount_pred_2 = Predicate{{lo, "discount"},
                                   arrow::compute::CompareOperator::LESS_EQUAL,
                                   arrow::Datum((uint8_t)6)};
  auto discount_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_2));

  auto discount_connective_node = std::make_shared<ConnectiveNode>(
      discount_pred_node_1, discount_pred_node_2, FilterOperator::AND);

  // quantity >= 26
  auto quantity_pred_1 =
      Predicate{{lo, "quantity"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum((uint8_t)26)};
  auto quantity_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(quantity_pred_1));

  // quantity <= 35
  auto quantity_pred_2 = Predicate{{lo, "quantity"},
                                   arrow::compute::CompareOperator::LESS_EQUAL,
                                   arrow::Datum((uint8_t)35)};
  auto quantity_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(quantity_pred_2));

  auto quantity_connective_node = std::make_shared<ConnectiveNode>(
      quantity_pred_node_1, quantity_pred_node_2, FilterOperator::AND);

  auto lo_root_node = std::make_shared<ConnectiveNode>(
      quantity_connective_node, discount_connective_node, FilterOperator::AND);

  auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

  // date.year month num = 199401
  ColumnReference year_ref = {d, "year month num"};
  auto year_pred_1 = Predicate{{d, "year month num"},
                               arrow::compute::CompareOperator::EQUAL,
                               arrow::Datum((int64_t)199401)};
  auto year_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(year_pred_1));
  auto d_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  Select lo_select_op(0, lo, lo_result_in, lo_select_result_out, lo_pred_tree);
  Select d_select_op(0, d, d_result_in, d_select_result_out, d_pred_tree);

  join_result_in = {lo_select_result_out, d_select_result_out};

  JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
  JoinGraph graph({{join_pred}});

  join_result_in = {lo_select_result_out, d_select_result_out};

  LIP lip_op(0, lip_result_in, lip_result_out, graph);
  Join join_op(0, {lip_result_out}, join_result_out, graph, join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {},
                   aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto lo_select_id = plan.addOperator(&lo_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);
  auto lip_id = plan.addOperator(&lip_op);
  auto join_id = plan.addOperator(&join_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(lo_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  plan.createLink(lip_id, join_id);

  // Declare aggregate dependency on join operator
  plan.createLink(join_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("1.2_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("1.2_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q13_lip() {
  // discount >= 5
  auto discount_pred_1 =
      Predicate{{lo, "discount"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum((uint8_t)5)};
  auto discount_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_1));

  // discount <= 7
  auto discount_pred_2 = Predicate{{lo, "discount"},
                                   arrow::compute::CompareOperator::LESS_EQUAL,
                                   arrow::Datum((uint8_t)7)};
  auto discount_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(discount_pred_2));

  auto discount_connective_node = std::make_shared<ConnectiveNode>(
      discount_pred_node_1, discount_pred_node_2, FilterOperator::AND);

  // quantity >= 26
  auto quantity_pred_1 =
      Predicate{{lo, "quantity"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum((uint8_t)26)};
  auto quantity_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(quantity_pred_1));

  // quantity <= 35
  auto quantity_pred_2 = Predicate{{lo, "quantity"},
                                   arrow::compute::CompareOperator::LESS_EQUAL,
                                   arrow::Datum((uint8_t)35)};
  auto quantity_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(quantity_pred_2));

  auto quantity_connective_node = std::make_shared<ConnectiveNode>(
      quantity_pred_node_1, quantity_pred_node_2, FilterOperator::AND);

  auto lo_root_node = std::make_shared<ConnectiveNode>(
      quantity_connective_node, discount_connective_node, FilterOperator::AND);

  auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

  // date.year month num = 199401
  auto d_pred_1 = Predicate{{d, "week num in year"},
                            arrow::compute::CompareOperator::EQUAL,
                            arrow::Datum((int64_t)6)};
  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  // date.year month num = 199401
  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::EQUAL,
                            arrow::Datum((int64_t)1994)};
  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::AND);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  Select lo_select_op(0, lo, lo_result_in, lo_select_result_out, lo_pred_tree);
  Select d_select_op(0, d, d_result_in, d_select_result_out, d_pred_tree);

  join_result_in = {lo_select_result_out, d_select_result_out};

  JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
  JoinGraph graph({{join_pred}});

  lip_result_in = {lo_select_result_out, d_select_result_out};

  LIP lip_op(0, lip_result_in, lip_result_out, graph);
  Join join_op(0, {lip_result_out}, join_result_out, graph, join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {},
                   aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto lo_select_id = plan.addOperator(&lo_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);
  auto lip_id = plan.addOperator(&lip_op);
  auto join_id = plan.addOperator(&join_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(lo_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  plan.createLink(lip_id, join_id);

  // Declare aggregate dependency on join operator
  plan.createLink(join_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("1.3_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("1.3_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q21_lip() {
  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto p_pred_1 =
      Predicate{{p, "category"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#12"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out, nullptr,
                              d_join_pred.right_col_ref_);
  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out};

  JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {{d, "year"}, {p, "brand1"}}, {{d, "year"}, {p, "brand1"}},
                   aggregate_options);

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(d_select_id, lip_id);
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);

  plan.createLink(lip_id, agg_id);

  // Declare aggregate dependency on join operator

  ////////////////////////////////////////////////////////////////////////////

  scheduler->addTask(&plan);

  auto container = hustle::simple_profiler.getContainer();
  container->startEvent("2.1_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("2.1_lip");

  if (print_) {
    out_table = agg_result_out->materialize(
        {{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "brand1"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
  }
  simple_profiler.clear();
  reset_results();
}

void SSB::q22_lip() {
  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("ASIA"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto p_pred_1 = Predicate{
      {p, "brand1"},
      arrow::compute::CompareOperator::GREATER_EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2221"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_2 = Predicate{
      {p, "brand1"},
      arrow::compute::CompareOperator::LESS_EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2228"))};
  auto p_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_2));

  auto p_connective_node = std::make_shared<ConnectiveNode>(
      p_pred_node_1, p_pred_node_2, FilterOperator::AND);

  auto p_pred_tree = std::make_shared<PredicateTree>(p_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out, nullptr,
                              d_join_pred.right_col_ref_);
  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out};

  JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {{d, "year"}, {p, "brand1"}}, {{d, "year"}, {p, "brand1"}},
                   aggregate_options);

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  ////////////////////////////////////////////////////////////////////////////

  scheduler->addTask(&plan);

  auto container = hustle::simple_profiler.getContainer();
  container->startEvent("2.2_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("2.2_lip");

  if (print_) {
    out_table = agg_result_out->materialize(
        {{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "brand1"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
  }
  simple_profiler.clear();
  reset_results();
}

void SSB::q23_lip() {
  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("EUROPE"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto p_pred_1 = Predicate{
      {p, "brand1"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2221"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out, nullptr,
                              d_join_pred.right_col_ref_);
  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out};

  JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {{d, "year"}, {p, "brand1"}}, {{d, "year"}, {p, "brand1"}},
                   aggregate_options);

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  ////////////////////////////////////////////////////////////////////////////

  scheduler->addTask(&plan);

  auto container = hustle::simple_profiler.getContainer();
  container->startEvent("2.3_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("2.3_lip");

  if (print_) {
    out_table = agg_result_out->materialize(
        {{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "brand1"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
  }
  simple_profiler.clear();
  reset_results();
}

void SSB::q31_lip() {
  auto d_pred_1 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::GREATER_EQUAL,
                            arrow::Datum((int64_t)1992)};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::LESS_EQUAL,
                            arrow::Datum((int64_t)1997)};

  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_2));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::AND);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("ASIA"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto c_pred_1 =
      Predicate{{c, "c region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("ASIA"))};
  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   s_select_result_out, c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, c_nation_ref, s_nation_ref},
                   {d_year_ref, {nullptr, "revenue"}}, aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("3.1_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("3.1_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                             {nullptr, "year"},
                                             {nullptr, "c nation"},
                                             {nullptr, "s nation"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q32_lip() {
  auto d_pred_1 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::GREATER_EQUAL,
                            arrow::Datum((int64_t)1992)};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::LESS_EQUAL,
                            arrow::Datum((int64_t)1997)};

  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_2));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::AND);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  auto s_pred_1 = Predicate{
      {s, "s nation"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED STATES"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto c_pred_1 = Predicate{
      {c, "c nation"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED STATES"))};
  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   s_select_result_out, c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, c_city_ref, s_city_ref},
                   {d_year_ref, {nullptr, "revenue"}}, aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("3.2_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("3.2_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                             {nullptr, "year"},
                                             {nullptr, "c city"},
                                             {nullptr, "s city"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q33_lip() {
  auto d_pred_1 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::GREATER_EQUAL,
                            arrow::Datum((int64_t)1992)};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::LESS_EQUAL,
                            arrow::Datum((int64_t)1997)};

  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_2));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::AND);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  auto s_pred_1 = Predicate{
      {s, "s city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI1"))};

  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_2 = Predicate{
      {s, "s city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI5"))};

  auto s_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_2));

  auto s_connective_node = std::make_shared<ConnectiveNode>(
      s_pred_node_1, s_pred_node_2, FilterOperator::OR);

  auto s_pred_tree = std::make_shared<PredicateTree>(s_connective_node);

  auto c_pred_1 = Predicate{
      {c, "c city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI1"))};

  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_2 = Predicate{
      {c, "c city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI5"))};

  auto c_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_2));

  auto c_connective_node = std::make_shared<ConnectiveNode>(
      c_pred_node_1, c_pred_node_2, FilterOperator::OR);

  auto c_pred_tree = std::make_shared<PredicateTree>(c_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   s_select_result_out, c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, c_city_ref, s_city_ref},
                   {d_year_ref, {nullptr, "revenue"}}, aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("3.3_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("3.3_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                             {nullptr, "year"},
                                             {nullptr, "c city"},
                                             {nullptr, "s city"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q34_lip() {
  auto d_pred_1 =
      Predicate{{d, "year month"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("Dec1997"))};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_tree = std::make_shared<PredicateTree>(d_pred_node_1);

  auto s_pred_1 = Predicate{
      {s, "s city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI1"))};

  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_2 = Predicate{
      {s, "s city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI5"))};

  auto s_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_2));

  auto s_connective_node = std::make_shared<ConnectiveNode>(
      s_pred_node_1, s_pred_node_2, FilterOperator::OR);

  auto s_pred_tree = std::make_shared<PredicateTree>(s_connective_node);

  auto c_pred_1 = Predicate{
      {c, "c city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI1"))};

  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_2 = Predicate{
      {c, "c city"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED KI5"))};

  auto c_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_2));

  auto c_connective_node = std::make_shared<ConnectiveNode>(
      c_pred_node_1, c_pred_node_2, FilterOperator::OR);

  auto c_pred_tree = std::make_shared<PredicateTree>(c_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   s_select_result_out, c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, c_city_ref, s_city_ref},
                   {d_year_ref, {nullptr, "revenue"}}, aggregate_options);

  ExecutionPlan plan(0);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("3.4_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("3.4_lip");

  if (print_) {
    out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                             {nullptr, "year"},
                                             {nullptr, "c city"},
                                             {nullptr, "s city"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q41_lip() {
  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto c_pred_1 =
      Predicate{{c, "c region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

  auto p_pred_1 =
      Predicate{{p, "mfgr"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#1"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_2 =
      Predicate{{p, "mfgr"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2"))};
  auto p_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_2));

  auto p_connective_node = std::make_shared<ConnectiveNode>(
      p_pred_node_1, p_pred_node_2, FilterOperator::OR);

  auto p_pred_tree = std::make_shared<PredicateTree>(p_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out, nullptr,
                              d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out,
                   c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, c_nation_ref}, {d_year_ref, c_nation_ref},
                   aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  ////////////////////////////////////////////////////////////////////////////

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("4.1_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("4.1_lip");

  out_table = agg_result_out->materialize(
      {{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c nation"}});
  if (print_) {
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q42_lip() {
  auto d_pred_1 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::GREATER_EQUAL,
                            arrow::Datum((int64_t)1997)};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::LESS_EQUAL,
                            arrow::Datum((int64_t)1998)};

  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_2));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::AND);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  auto s_pred_1 =
      Predicate{{s, "s region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto c_pred_1 =
      Predicate{{c, "c region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

  auto p_pred_1 =
      Predicate{{p, "mfgr"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#1"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_2 =
      Predicate{{p, "mfgr"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2"))};
  auto p_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_2));

  auto p_connective_node = std::make_shared<ConnectiveNode>(
      p_pred_node_1, p_pred_node_2, FilterOperator::OR);

  auto p_pred_tree = std::make_shared<PredicateTree>(p_connective_node);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out,
                   c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, {lip_result_out}, agg_result_out, {agg_ref},
                   {d_year_ref, s_nation_ref, p_category_ref},
                   {d_year_ref, s_nation_ref, p_category_ref},
                   aggregate_options);

  ////////////////////////////////////////////////////////////////////////////

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("4.2_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("4.2_lip");

  out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                           {nullptr, "year"},
                                           {nullptr, "s nation"},
                                           {nullptr, "category"}});
  if (print_) {
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

void SSB::q43_lip() {
  auto d_pred_1 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::EQUAL,
                            arrow::Datum((int64_t)1997)};

  auto d_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_1));

  auto d_pred_2 = Predicate{{d, "year"},
                            arrow::compute::CompareOperator::EQUAL,
                            arrow::Datum((int64_t)1998)};

  auto d_pred_node_2 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(d_pred_2));

  auto d_connective_node = std::make_shared<ConnectiveNode>(
      d_pred_node_1, d_pred_node_2, FilterOperator::OR);

  auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

  auto s_pred_1 = Predicate{
      {s, "s nation"},
      arrow::compute::CompareOperator::EQUAL,
      arrow::Datum(std::make_shared<arrow::StringScalar>("UNITED STATES"))};
  auto s_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(s_pred_1));

  auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

  auto c_pred_1 =
      Predicate{{c, "c region"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("AMERICA"))};
  auto c_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(c_pred_1));

  auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

  auto p_pred_1 =
      Predicate{{p, "category"},
                arrow::compute::CompareOperator::EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#14"))};
  auto p_pred_node_1 =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(p_pred_1));

  auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

  ////////////////////////////////////////////////////////////////////////////

  lo_select_result_out->append(lo);

  SelectBuildHash p_select_op(0, p, p_result_in, p_select_result_out,
                              p_pred_tree, p_join_pred.right_col_ref_);
  SelectBuildHash s_select_op(0, s, s_result_in, s_select_result_out,
                              s_pred_tree, s_join_pred.right_col_ref_);
  SelectBuildHash c_select_op(0, c, c_result_in, c_select_result_out,
                              c_pred_tree, c_join_pred.right_col_ref_);
  SelectBuildHash d_select_op(0, d, d_result_in, d_select_result_out,
                              d_pred_tree, d_join_pred.right_col_ref_);

  lip_result_in = {lo_select_result_out, d_select_result_out,
                   p_select_result_out, s_select_result_out,
                   c_select_result_out};

  JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
  FilterJoin lip_op(0, lip_result_in, lip_result_out, graph,
                    filter_join_options);

  AggregateReference agg_ref = {AggregateKernel::SUM, "revenue", lo_rev_ref};
  Aggregate agg_op(0, lip_result_out, agg_result_out, {agg_ref},
                   {d_year_ref, s_city_ref, p_brand1_ref},
                   {d_year_ref, s_city_ref, p_brand1_ref}, aggregate_options);

  ExecutionPlan plan(0);
  auto p_select_id = plan.addOperator(&p_select_op);
  auto s_select_id = plan.addOperator(&s_select_op);
  auto c_select_id = plan.addOperator(&c_select_op);
  auto d_select_id = plan.addOperator(&d_select_op);

  auto lip_id = plan.addOperator(&lip_op);
  auto agg_id = plan.addOperator(&agg_op);

  // Declare join dependency on select operators
  plan.createLink(p_select_id, lip_id);
  plan.createLink(s_select_id, lip_id);
  plan.createLink(c_select_id, lip_id);
  plan.createLink(d_select_id, lip_id);

  // Declare aggregate dependency on join operator
  plan.createLink(lip_id, agg_id);

  scheduler->addTask(&plan);

  auto container = simple_profiler.getContainer();
  container->startEvent("4.3_lip");
  scheduler->start();
  scheduler->join();
  container->endEvent("4.3_lip");

  out_table = agg_result_out->materialize({{nullptr, "revenue"},
                                           {nullptr, "year"},
                                           {nullptr, "s city"},
                                           {nullptr, "brand1"}});
  if (print_) {
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();
  reset_results();
}

}  // namespace hustle::operators