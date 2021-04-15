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

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>
#include <memory>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/join/hash_join.h"
#include "optimizer/reorder_joins.h"
#include "storage/utils/util.h"

using namespace testing;
using namespace hustle;
using namespace hustle::operators;
using namespace hustle::optimizer;

class ReorderingTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> s_schema, r_schema, t_schema, q_schema,
      u_schema, v_schema;

  std::shared_ptr<HustleTable> Q, R, S, T, U, V;

  void SetUp() override {
    arrow::Status status;

    auto r_field_1 = arrow::field("rkey", arrow::int64());
    auto r_field_2 = arrow::field("rdata", arrow::utf8());
    auto s_field_1 = arrow::field("skey", arrow::int64());
    auto s_field_2 = arrow::field("sdata", arrow::utf8());
    auto t_field_1 = arrow::field("tkey", arrow::int64());
    auto t_field_2 = arrow::field("tdata", arrow::utf8());

    auto q_field_1 = arrow::field("qkey", arrow::int64());
    auto q_field_2 = arrow::field("qdata", arrow::utf8());
    auto u_field_1 = arrow::field("ukey", arrow::int64());
    auto u_field_2 = arrow::field("udata", arrow::utf8());

    auto v_field_1 = arrow::field("vkey", arrow::int64());
    auto v_field_2 = arrow::field("vdata", arrow::utf8());

    s_schema = arrow::schema({s_field_1, s_field_2});
    r_schema = arrow::schema({r_field_1, r_field_2});
    t_schema = arrow::schema({t_field_1, t_field_2});
    q_schema = arrow::schema({q_field_1, q_field_2});
    u_schema = arrow::schema({u_field_1, u_field_2});
    v_schema = arrow::schema({v_field_1, v_field_2});

    std::ofstream R_csv;
    std::ofstream S_csv;
    std::ofstream T_csv;
    std::ofstream Q_csv;
    std::ofstream U_csv;
    std::ofstream V_csv;

    Q_csv.open("Q.csv");
    R_csv.open("R.csv");
    S_csv.open("S.csv");
    T_csv.open("T.csv");
    U_csv.open("V.csv");
    V_csv.open("U.csv");

    for (int i = 0; i < 3; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string(i) << std::endl;
    }
    R_csv.close();

    for (int i = 0; i < 4; i++) {
      S_csv << std::to_string(3 - i) << "|";
      S_csv << "S" << std::to_string(3 - i) << std::endl;
    }
    S_csv.close();

    for (int i = 0; i < 5; i++) {
      T_csv << std::to_string(i) << "|";
      T_csv << "T" << std::to_string(i) << std::endl;
    }
    T_csv.close();

    for (int i = 0; i < 2; i++) {
      Q_csv << std::to_string(i) << "|";
      Q_csv << "Q" << std::to_string(i) << std::endl;
    }
    Q_csv.close();

    for (int i = 0; i < 2; i++) {
      U_csv << std::to_string(i) << "|";
      U_csv << "U" << std::to_string(i) << std::endl;
    }
    U_csv.close();

    for (int i = 0; i < 2; i++) {
      V_csv << std::to_string(i) << "|";
      V_csv << "V" << std::to_string(i) << std::endl;
    }
    V_csv.close();
  }
};

TEST_F(ReorderingTestFixture, NumJoin2) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", t_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  ColumnReference T_ref_1 = {T, "tkey"};
  ColumnReference T_ref_2 = {T, "tdata"};

  auto out_result = std::make_shared<OperatorResult>();
  auto prev_out_result1 = std::make_shared<OperatorResult>();
  auto prev_out_result2 = std::make_shared<OperatorResult>();

  prev_out_result1->append(R);
  prev_out_result2->append(S);

  std::vector<OperatorResult::OpResultPtr> input_results;
  input_results.emplace_back(prev_out_result1);
  input_results.emplace_back(prev_out_result2);

  std::shared_ptr<JoinPredicate> join_pred_RS = std::make_shared<JoinPredicate>(
      JoinPredicate{R_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::shared_ptr<JoinPredicate> join_pred_TS = std::make_shared<JoinPredicate>(
      JoinPredicate{T_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::shared_ptr<hustle::ExecutionPlan> plan =
      ReorderJoin::ApplyJoinReordering(0, {*join_pred_TS, *join_pred_RS},
                                       input_results);
  EXPECT_EQ(plan->size(), 2);
  HashJoin& join1 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(0).get()));
  HashJoin& join2 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(1).get()));

  EXPECT_EQ(join1.predicate()->right_col_.col_name, S_ref_1.col_name);
  EXPECT_EQ(join2.predicate()->right_col_.col_name, S_ref_1.col_name);

  EXPECT_EQ(join1.predicate()->left_col_.col_name, R_ref_1.col_name);
  EXPECT_EQ(join2.predicate()->left_col_.col_name, T_ref_1.col_name);
}

TEST_F(ReorderingTestFixture, NumJoin4) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", t_schema, BLOCK_SIZE);
  Q = read_from_csv_file("Q.csv", q_schema, BLOCK_SIZE);
  U = read_from_csv_file("U.csv", u_schema, BLOCK_SIZE);
  V = read_from_csv_file("V.csv", v_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  ColumnReference T_ref_1 = {T, "tkey"};
  ColumnReference T_ref_2 = {T, "tdata"};

  ColumnReference U_ref_1 = {U, "ukey"};
  ColumnReference U_ref_2 = {U, "udata"};

  ColumnReference V_ref_1 = {V, "vkey"};
  ColumnReference V_ref_2 = {V, "vdata"};

  auto out_result = std::make_shared<OperatorResult>();
  auto prev_out_result1 = std::make_shared<OperatorResult>();
  auto prev_out_result2 = std::make_shared<OperatorResult>();
  auto prev_out_result3 = std::make_shared<OperatorResult>();
  auto prev_out_result4 = std::make_shared<OperatorResult>();
  auto prev_out_result5 = std::make_shared<OperatorResult>();

  prev_out_result1->append(R);
  prev_out_result2->append(S);
  prev_out_result3->append(T);
  prev_out_result4->append(U);
  prev_out_result5->append(V);

  std::vector<OperatorResult::OpResultPtr> input_results(
      {prev_out_result1, prev_out_result2, prev_out_result3, prev_out_result4,
       prev_out_result5});

  std::shared_ptr<JoinPredicate> join_pred_RS = std::make_shared<JoinPredicate>(
      JoinPredicate{V_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::shared_ptr<JoinPredicate> join_pred_TS = std::make_shared<JoinPredicate>(
      JoinPredicate{T_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::shared_ptr<JoinPredicate> join_pred_US = std::make_shared<JoinPredicate>(
      JoinPredicate{U_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::shared_ptr<JoinPredicate> join_pred_RV = std::make_shared<JoinPredicate>(
      JoinPredicate{R_ref_1, arrow::compute::EQUAL, V_ref_1});

  std::shared_ptr<hustle::ExecutionPlan> plan =
      ReorderJoin::ApplyJoinReordering(
          0, {*join_pred_RS, *join_pred_TS, *join_pred_RV, *join_pred_US},
          input_results);
  EXPECT_EQ(plan->size(), 4);

  HashJoin& join1 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(0).get()));
  HashJoin& join2 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(1).get()));
  HashJoin& join3 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(2).get()));
  HashJoin& join4 =
      *(static_cast<HashJoin*>(plan->getAllOperators().at(3).get()));

  EXPECT_EQ(join1.predicate()->right_col_.col_name, V_ref_1.col_name);
  EXPECT_EQ(join2.predicate()->right_col_.col_name, S_ref_1.col_name);
  EXPECT_EQ(join3.predicate()->right_col_.col_name, S_ref_1.col_name);
  EXPECT_EQ(join4.predicate()->right_col_.col_name, S_ref_1.col_name);

  EXPECT_EQ(join1.predicate()->left_col_.col_name, R_ref_1.col_name);
  EXPECT_EQ(join2.predicate()->left_col_.col_name, V_ref_1.col_name);
  EXPECT_EQ(join3.predicate()->left_col_.col_name, U_ref_1.col_name);
  EXPECT_EQ(join4.predicate()->left_col_.col_name, T_ref_1.col_name);
}
