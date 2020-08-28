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

#include "operators/join.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/select.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/util.h"

using namespace testing;
using namespace hustle;
using namespace hustle::operators;

class JoinTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> schema;

  arrow::Int64Builder int_builder;
  arrow::StringBuilder str_builder;
  std::shared_ptr<arrow::Array> expected_R_col_1;
  std::shared_ptr<arrow::Array> expected_R_col_2;
  std::shared_ptr<arrow::Array> expected_S_col_1;
  std::shared_ptr<arrow::Array> expected_S_col_2;
  std::shared_ptr<arrow::Array> expected_T_col_1;
  std::shared_ptr<arrow::Array> expected_T_col_2;

  std::shared_ptr<Table> R, S, T;

  void SetUp() override {
    arrow::Status status;

    auto field_1 = arrow::field("key", arrow::int64());
    auto field_2 = arrow::field("data", arrow::utf8());

    schema = arrow::schema({field_1, field_2});

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
  }
};

/*
 * SELECT *
 * FROM R, S
 * WHERE R.key == S.key
 */
TEST_F(JoinTestFixture, EquiJoin1) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "key"};
  ColumnReference R_ref_2 = {R, "data"};

  ColumnReference S_ref_1 = {S, "key"};
  ColumnReference S_ref_2 = {S, "data"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);

  JoinPredicate join_pred = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinGraph graph({{join_pred}});
  Join join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table =
      out_result->materialize({R_ref_1, R_ref_2, S_ref_1, S_ref_2});
  //    out_table->print();

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
 * SELECT *
 * FROM R, S, T
 * WHERE R.key == S.key AND
 *       R.key == T.key
 */
TEST_F(JoinTestFixture, EquiJoin2) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "key"};
  ColumnReference R_ref_2 = {R, "data"};

  ColumnReference S_ref_1 = {S, "key"};
  ColumnReference S_ref_2 = {S, "data"};

  ColumnReference T_ref_1 = {T, "key"};
  ColumnReference T_ref_2 = {T, "data"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);
  result->append(T);

  JoinPredicate join_pred_RS = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinPredicate join_pred_RT = {R_ref_1, arrow::compute::EQUAL, T_ref_1};

  JoinGraph graph({{join_pred_RS, join_pred_RT}});
  Join join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize(
      {R_ref_1, R_ref_2, S_ref_1, S_ref_2, T_ref_1, T_ref_2});
  ////    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_S_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_T_col_1);

  status = str_builder.AppendValues({"R0", "R1", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = str_builder.AppendValues({"S0", "S1", "S2"});
  status = str_builder.Finish(&expected_S_col_2);

  status = str_builder.AppendValues({"T0", "T1", "T2"});
  status = str_builder.Finish(&expected_T_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_S_col_1));
  EXPECT_TRUE(out_table->get_column(3)->chunk(0)->Equals(expected_S_col_2));
  EXPECT_TRUE(out_table->get_column(4)->chunk(0)->Equals(expected_T_col_1));
  EXPECT_TRUE(out_table->get_column(5)->chunk(0)->Equals(expected_T_col_2));
}

/*
 * SELECT *
 * FROM R, S, T
 * WHERE R.key == S.key AND
 *       S.key == T.key
 */
TEST_F(JoinTestFixture, EquiJoin3) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "key"};
  ColumnReference R_ref_2 = {R, "data"};

  ColumnReference S_ref_1 = {S, "key"};
  ColumnReference S_ref_2 = {S, "data"};

  ColumnReference T_ref_1 = {T, "key"};
  ColumnReference T_ref_2 = {T, "data"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);
  result->append(T);

  JoinPredicate join_pred_RS = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinPredicate join_pred_ST = {S_ref_1, arrow::compute::EQUAL, T_ref_1};

  JoinGraph graph({{join_pred_RS}, {join_pred_ST}});
  Join join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize(
      {R_ref_1, R_ref_2, S_ref_1, S_ref_2, T_ref_1, T_ref_2});
  ////    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_S_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_T_col_1);

  status = str_builder.AppendValues({"R0", "R1", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = str_builder.AppendValues({"S0", "S1", "S2"});
  status = str_builder.Finish(&expected_S_col_2);

  status = str_builder.AppendValues({"T0", "T1", "T2"});
  status = str_builder.Finish(&expected_T_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_S_col_1));
  EXPECT_TRUE(out_table->get_column(3)->chunk(0)->Equals(expected_S_col_2));
  EXPECT_TRUE(out_table->get_column(4)->chunk(0)->Equals(expected_T_col_1));
  EXPECT_TRUE(out_table->get_column(5)->chunk(0)->Equals(expected_T_col_2));
}