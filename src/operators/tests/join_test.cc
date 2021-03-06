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
#include "operators/join/multiway_join.h"
#include "operators/select/select.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/utils/util.h"

using namespace testing;
using namespace hustle;
using namespace hustle::operators;

class JoinTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> q_schema, s_schema, r_schema, t_schema;

  arrow::Int64Builder int_builder;
  arrow::StringBuilder str_builder;
  std::shared_ptr<arrow::Array> expected_Q_col_1;
  std::shared_ptr<arrow::Array> expected_Q_col_2;
  std::shared_ptr<arrow::Array> expected_R_col_1;
  std::shared_ptr<arrow::Array> expected_R_col_2;
  std::shared_ptr<arrow::Array> expected_S_col_1;
  std::shared_ptr<arrow::Array> expected_S_col_2;
  std::shared_ptr<arrow::Array> expected_T_col_1;
  std::shared_ptr<arrow::Array> expected_T_col_2;

  DBTable::TablePtr Q, R, S, T;

  void SetUp() override {
    arrow::Status status;

    auto q_field_1 = arrow::field("qkey", arrow::int64());
    auto q_field_2 = arrow::field("qdata", arrow::utf8());
    auto r_field_1 = arrow::field("rkey", arrow::int64());
    auto r_field_2 = arrow::field("rdata", arrow::utf8());
    auto s_field_1 = arrow::field("skey", arrow::int64());
    auto s_field_2 = arrow::field("sdata", arrow::utf8());
    auto t_field_1 = arrow::field("tkey", arrow::int64());
    auto t_field_2 = arrow::field("tdata", arrow::utf8());

    q_schema = arrow::schema({q_field_1, q_field_2});
    s_schema = arrow::schema({s_field_1, s_field_2});
    r_schema = arrow::schema({r_field_1, r_field_2});
    t_schema = arrow::schema({t_field_1, t_field_2});

    std::ofstream Q_csv;
    std::ofstream R_csv;
    std::ofstream S_csv;
    std::ofstream T_csv;
    Q_csv.open("Q.csv");
    R_csv.open("R.csv");
    S_csv.open("S.csv");
    T_csv.open("T.csv");

    for (int i = 1; i < 3; i++) {
      Q_csv << std::to_string(i) << "|";
      Q_csv << "R" << std::to_string(i) << std::endl;
    }
    Q_csv.close();

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
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);

  JoinPredicate join_pred = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinGraph graph({{join_pred}});
  MultiwayJoin join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

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
 * SELECT *
 * FROM R, Q
 * WHERE R.data == S.data
 */
TEST_F(JoinTestFixture, EquiJoin1_TypeString) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  Q = read_from_csv_file("Q.csv", q_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference Q_ref_1 = {Q, "qkey"};
  ColumnReference Q_ref_2 = {Q, "qdata"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(Q);

  JoinPredicate join_pred = {R_ref_1, arrow::compute::EQUAL, Q_ref_1};
  JoinGraph graph({{join_pred}});
  MultiwayJoin join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table =
      out_result->materialize({R_ref_1, R_ref_2, Q_ref_1, Q_ref_2});

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({1, 2});
  status = int_builder.Finish(&expected_R_col_1);

  status = int_builder.AppendValues({1, 2});
  status = int_builder.Finish(&expected_Q_col_1);

  status = str_builder.AppendValues({"R1", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = str_builder.AppendValues({"R1", "R2"});
  status = str_builder.Finish(&expected_Q_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_Q_col_1));
  EXPECT_TRUE(out_table->get_column(3)->chunk(0)->Equals(expected_Q_col_2));
}

/*
 * SELECT R.key, S.data
 * FROM R, S, T
 * WHERE R.key == S.key AND
 *       R.key == T.key
 */
TEST_F(JoinTestFixture, EquiJoin2) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", t_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  ColumnReference T_ref_1 = {T, "tkey"};
  ColumnReference T_ref_2 = {T, "tdata"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);
  result->append(T);

  JoinPredicate join_pred_RS = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinPredicate join_pred_RT = {R_ref_1, arrow::compute::EQUAL, T_ref_1};

  JoinGraph graph({{join_pred_RS, join_pred_RT}});
  MultiwayJoin join_op(0, {result}, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(join_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({R_ref_1, S_ref_1, T_ref_2});
  // Construct expected results
  arrow::Status status;

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);

  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_S_col_1);

  status = str_builder.AppendValues({"T0", "T1", "T2"});
  status = str_builder.Finish(&expected_T_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_S_col_1));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_T_col_2));
}

/*
 * SELECT R.key, T.data
 * FROM R, S, T
 * WHERE T.key == S.key AND
 *       T.key == R.key
 */
TEST_F(JoinTestFixture, EquiJoin3) {
  R = read_from_csv_file("R.csv", r_schema, BLOCK_SIZE);
  S = read_from_csv_file("S.csv", s_schema, BLOCK_SIZE);
  T = read_from_csv_file("T.csv", t_schema, BLOCK_SIZE);

  ColumnReference R_ref_1 = {R, "rkey"};
  ColumnReference R_ref_2 = {R, "rdata"};

  ColumnReference S_ref_1 = {S, "skey"};
  ColumnReference S_ref_2 = {S, "sdata"};

  ColumnReference T_ref_1 = {T, "tkey"};
  ColumnReference T_ref_2 = {T, "tdata"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);
  result->append(S);
  result->append(T);

  JoinPredicate join_pred_TS = {T_ref_1, arrow::compute::EQUAL, S_ref_1};
  JoinPredicate join_pred_TR = {T_ref_1, arrow::compute::EQUAL, R_ref_1};

  JoinGraph graph({{join_pred_TS, join_pred_TR}});
  std::vector<std::shared_ptr<OperatorResult>> out_result_vec1;
  out_result_vec1.push_back(result);
  std::unique_ptr<MultiwayJoin> join_op =
      std::make_unique<MultiwayJoin>(0, out_result_vec1, out_result, graph);

  Scheduler &scheduler = Scheduler::GlobalInstance();
  auto next_out_result = std::make_shared<OperatorResult>();

  std::shared_ptr<hustle::ExecutionPlan> plan =
      std::make_shared<hustle::ExecutionPlan>(0);
  plan->addOperator(join_op.get());

  scheduler.addTask(plan.get());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({R_ref_1, T_ref_2});
  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 1, 2});
  status = int_builder.Finish(&expected_R_col_1);
  status = str_builder.AppendValues({"T0", "T1", "T2"});
  status = str_builder.Finish(&expected_T_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_T_col_2));
}
