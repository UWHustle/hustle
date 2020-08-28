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

#include "operators/select.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/util.h"

#define BLOCK_SIZE 108

using namespace testing;
using namespace hustle;
using namespace hustle::operators;

class SelectTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> schema;

  arrow::Int64Builder int_builder;
  arrow::DoubleBuilder double_builder;
  arrow::StringBuilder str_builder;
  std::shared_ptr<arrow::Array> expected_R_col_1;
  std::shared_ptr<arrow::Array> expected_R_col_2;
  std::shared_ptr<arrow::Array> expected_R_col_3;

  std::shared_ptr<Table> R, S, T;

  void SetUp() override {
    arrow::Status status;

    auto field_1 = arrow::field("key", arrow::int64());
    auto field_2 = arrow::field("group", arrow::utf8());
    auto field_3 = arrow::field("data", arrow::int64());

    schema = arrow::schema({field_1, field_2, field_3});

    std::ofstream R_csv;
    std::ofstream S_csv;
    std::ofstream T_csv;
    R_csv.open("R.csv");
    S_csv.open("S.csv");

    for (int i = 0; i < 6; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string(i / 2) << "|";
      R_csv << std::to_string(i * 10) << std::endl;
    }
    R_csv.close();

    for (int i = 0; i < 6; i++) {
      S_csv << std::to_string(i) << "|";
      S_csv << "R" << std::to_string(i / 2) << "|";
      S_csv << std::to_string(i * 10) << std::endl;
      for (int j = 0; j < 6; j++) {
        S_csv << "-1|AA|-1" << std::endl;
      }
    }
    S_csv.close();
  }
};

/*
 * SELECT *
 * FROM R
 * WHERE R.group >= "R1"
 */
TEST_F(SelectTestFixture, SingleSelectTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  auto select_pred =
      Predicate{{R, "group"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("R1"))};

  auto select_pred_node =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(select_pred));

  auto select_pred_tree = std::make_shared<PredicateTree>(select_pred_node);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  Select select_op(0, result, out_result, select_pred_tree);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(select_op.createTask());
  scheduler.start();

  scheduler.join();

  auto out_table =
      out_result->materialize({R_key_ref, R_group_ref, R_data_ref});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({2, 3, 4, 5});
  status = int_builder.Finish(&expected_R_col_1);

  status = str_builder.AppendValues({"R1", "R1", "R2", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = int_builder.AppendValues({20, 30, 40, 50});
  status = int_builder.Finish(&expected_R_col_3);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_R_col_3));
}

/*
 * SELECT *
 * FROM R
 * WHERE R.group >= "R1" AND
 *       R.data <= 30
 */
TEST_F(SelectTestFixture, AndSelectTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  auto select_pred_1 =
      Predicate{{R, "group"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("R1"))};

  auto select_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_1));

  // NOTE: Make sure you cast integer values to int64_t when constructing
  // an integer Datum.
  auto select_pred_2 = Predicate{{R, "data"},
                                 arrow::compute::CompareOperator::LESS_EQUAL,
                                 arrow::Datum((int64_t)30)};

  auto select_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_2));

  auto select_connective_node =
      std::make_shared<ConnectiveNode>(select_pred_node_1, select_pred_node_2,
                                       hustle::operators::FilterOperator::AND);

  auto select_pred_tree =
      std::make_shared<PredicateTree>(select_connective_node);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  Select select_op(0, result, out_result, select_pred_tree);
  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(select_op.createTask());
  scheduler.start();

  scheduler.join();

  auto out_table =
      out_result->materialize({R_key_ref, R_group_ref, R_data_ref});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({2, 3});
  status = int_builder.Finish(&expected_R_col_1);

  status = str_builder.AppendValues({"R1", "R1"});
  status = str_builder.Finish(&expected_R_col_2);

  status = int_builder.AppendValues({20, 30});
  status = int_builder.Finish(&expected_R_col_3);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_R_col_3));
}

/*
 * SELECT *
 * FROM R
 * WHERE R.group >= "R1" OR
 *       R.data == 0
 */
TEST_F(SelectTestFixture, OrSelectTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  auto select_pred_1 =
      Predicate{{R, "group"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("R1"))};

  auto select_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_1));

  // NOTE: Make sure you cast integer values to int64_t when constructing
  // an integer Datum.
  auto select_pred_2 = Predicate{{R, "data"},
                                 arrow::compute::CompareOperator::EQUAL,
                                 arrow::Datum((int64_t)0)};

  auto select_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_2));

  auto select_connective_node =
      std::make_shared<ConnectiveNode>(select_pred_node_1, select_pred_node_2,
                                       hustle::operators::FilterOperator::OR);

  auto select_pred_tree =
      std::make_shared<PredicateTree>(select_connective_node);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  Select select_op(0, result, out_result, select_pred_tree);
  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(select_op.createTask());
  scheduler.start();

  scheduler.join();

  auto out_table =
      out_result->materialize({R_key_ref, R_group_ref, R_data_ref});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({0, 2, 3, 4, 5});
  status = int_builder.Finish(&expected_R_col_1);

  status = str_builder.AppendValues({"R0", "R1", "R1", "R2", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = int_builder.AppendValues({0, 20, 30, 40, 50});
  status = int_builder.Finish(&expected_R_col_3);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_R_col_3));
}

/*
 * SELECT *
 * FROM R
 * WHERE R.group >= "R1"
 */
TEST_F(SelectTestFixture, SingleSelectManyBlocksTest) {
  R = read_from_csv_file("S.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  auto select_pred =
      Predicate{{R, "group"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("R1"))};

  auto select_pred_node =
      std::make_shared<PredicateNode>(std::make_shared<Predicate>(select_pred));

  auto select_pred_tree = std::make_shared<PredicateTree>(select_pred_node);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  Select select_op(0, result, out_result, select_pred_tree);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(select_op.createTask());
  scheduler.start();

  scheduler.join();

  auto out_table =
      out_result->materialize({R_key_ref, R_group_ref, R_data_ref});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({2, 3, 4, 5});
  status = int_builder.Finish(&expected_R_col_1);

  status = str_builder.AppendValues({"R1", "R1", "R2", "R2"});
  status = str_builder.Finish(&expected_R_col_2);

  status = int_builder.AppendValues({20, 30, 40, 50});
  status = int_builder.Finish(&expected_R_col_3);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_R_col_3));
}

/*
 * SELECT *
 * FROM R
 * WHERE R.group >= "R1" AND
 *       R.data <= 30
 */
TEST_F(SelectTestFixture, AndSelectManyBlocksTest) {
  R = read_from_csv_file("S.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  auto select_pred_1 =
      Predicate{{R, "group"},
                arrow::compute::CompareOperator::GREATER_EQUAL,
                arrow::Datum(std::make_shared<arrow::StringScalar>("R1"))};

  auto select_pred_node_1 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_1));

  // NOTE: Make sure you cast integer values to int64_t when constructing
  // an integer Datum.
  auto select_pred_2 = Predicate{{R, "data"},
                                 arrow::compute::CompareOperator::LESS_EQUAL,
                                 arrow::Datum((int64_t)30)};

  auto select_pred_node_2 = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_pred_2));

  auto select_connective_node =
      std::make_shared<ConnectiveNode>(select_pred_node_1, select_pred_node_2,
                                       hustle::operators::FilterOperator::AND);

  auto select_pred_tree =
      std::make_shared<PredicateTree>(select_connective_node);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  Select select_op(0, result, out_result, select_pred_tree);

  Scheduler &scheduler = Scheduler::GlobalInstance();

  scheduler.addTask(select_op.createTask());
  scheduler.start();

  scheduler.join();

  auto out_table =
      out_result->materialize({R_key_ref, R_group_ref, R_data_ref});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.AppendValues({2, 3});
  status = int_builder.Finish(&expected_R_col_1);

  status = str_builder.AppendValues({"R1", "R1"});
  status = str_builder.Finish(&expected_R_col_2);

  status = int_builder.AppendValues({20, 30});
  status = int_builder.Finish(&expected_R_col_3);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
  EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_R_col_3));
}