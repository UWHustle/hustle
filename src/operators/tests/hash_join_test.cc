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

#include "operators/join/hash_join.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>
#include <memory>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/select/select.h"
#include "scheduler/scheduler.h"
#include "storage/base_block.h"
#include "storage/utils/util.h"

using namespace testing;
using namespace hustle;
using namespace hustle::operators;

class HashJoinTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> s_schema, r_schema, t_schema;

  arrow::Int64Builder int_builder;
  arrow::StringBuilder str_builder;
  std::shared_ptr<arrow::Array> expected_R_col_1;
  std::shared_ptr<arrow::Array> expected_R_col_2;
  std::shared_ptr<arrow::Array> expected_S_col_1;
  std::shared_ptr<arrow::Array> expected_S_col_2;
  std::shared_ptr<arrow::Array> expected_T_col_1;
  std::shared_ptr<arrow::Array> expected_T_col_2;

  std::shared_ptr<HustleTable> R, S, T;

  void SetUp() override {
    arrow::Status status;

    auto r_field_1 = arrow::field("rkey", arrow::int64());
    auto r_field_2 = arrow::field("rdata", arrow::utf8());
    auto s_field_1 = arrow::field("skey", arrow::int64());
    auto s_field_2 = arrow::field("sdata", arrow::utf8());
    auto t_field_1 = arrow::field("tkey", arrow::int64());
    auto t_field_2 = arrow::field("tdata", arrow::utf8());

    s_schema = arrow::schema({s_field_1, s_field_2});
    r_schema = arrow::schema({r_field_1, r_field_2});
    t_schema = arrow::schema({t_field_1, t_field_2});

    std::ofstream R_csv;
    std::ofstream S_csv;
    std::ofstream T_csv;
    R_csv.open("R.csv");
    S_csv.open("S.csv");
    T_csv.open("T.csv");

    for (int i = 0; i < 3; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string(i) << std::endl;
    }
    R_csv.close();

    for (int i = 0; i < 5; i++) {
      S_csv << std::to_string(3 - i) << "|";
      S_csv << "S" << std::to_string(3 - i) << std::endl;
    }
    S_csv.close();

    for (int i = 0; i < 5; i++) {
      T_csv << std::to_string(i) << "|";
      T_csv << "T" << std::to_string(i) << std::endl;
    }
    T_csv.close();
  }
};

/*
 * SELECT *
 * FROM R, S
 * WHERE R.key == S.key
 */
TEST_F(HashJoinTestFixture, EquiJoin1) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  auto result1 = std::make_shared<OperatorResult>();
  auto result2 = std::make_shared<OperatorResult>();

  auto out_result = std::make_shared<OperatorResult>();
  result1->append(R);
  result2->append(S);

  std::shared_ptr<JoinPredicate> join_pred = std::make_shared<JoinPredicate>(
      JoinPredicate{R_ref_1, arrow::compute::EQUAL, S_ref_1});
  HashJoin join_op(0, {result1, result2}, out_result, join_pred);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();
  std::cout << "size: " << out_result->lazy_tables_.size() << std::endl;
  auto out_table =
      out_result->materialize({R_ref_1, R_ref_2, S_ref_1, S_ref_2});

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_S_col_1);

  status = str_builder.AppendValues({"R0", "R1", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = str_builder.AppendValues({"S0", "S1", "S2"});
  status = str_builder.Finish(&expected_S_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_S_col_1));
  EXPECT_TRUE(out_table->get_column(3)->chunk(0)->Equals(expected_S_col_2));
}

/*
 * SELECT R.key, T.data
 * FROM R, S, T
 * WHERE R.key == S.key AND
 *       S.key == T.key
 */
TEST_F(HashJoinTestFixture, EquiJoin2) {
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

  std::shared_ptr<JoinPredicate> join_pred_RS = std::make_shared<JoinPredicate>(
      JoinPredicate{R_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::vector<std::shared_ptr<OperatorResult>> out_result_vec1;
  out_result_vec1.push_back(prev_out_result1);
  out_result_vec1.push_back(prev_out_result2);

  std::unique_ptr<HashJoin> join_op =
      std::make_unique<HashJoin>(0, out_result_vec1, out_result, join_pred_RS);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  auto next_out_result = std::make_shared<OperatorResult>();
  auto sec_output_result = std::make_shared<OperatorResult>();
  sec_output_result->append(T);

  std::shared_ptr<JoinPredicate> join_pred_OS = std::make_shared<JoinPredicate>(
      JoinPredicate{T_ref_1, arrow::compute::EQUAL, S_ref_1});

  std::vector<std::shared_ptr<OperatorResult>> out_result_vec;
  out_result_vec.push_back(sec_output_result);
  out_result_vec.push_back(out_result);

  std::unique_ptr<HashJoin> rs_join_op = std::make_unique<HashJoin>(
      0, out_result_vec, next_out_result, join_pred_OS);

  std::shared_ptr<hustle::ExecutionPlan> plan =
      std::make_shared<hustle::ExecutionPlan>(0);
  auto join_id_1 = plan->addOperator(join_op.get());
  auto join_id_2 = plan->addOperator(rs_join_op.get());

  plan->createLink(join_id_1, join_id_2);

  scheduler.addTask(plan.get());

  scheduler.start();
  scheduler.join();

  auto out_table = next_out_result->materialize({R_ref_1, T_ref_2});
  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);
  status = str_builder.AppendValues({"T0", "T1", "T2"});
  status = str_builder.Finish(&expected_T_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_T_col_2));
}
