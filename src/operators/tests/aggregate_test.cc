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

#include "operators/aggregate.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>
#include <operators/hash_aggregate.h>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/join.h"
#include "operators/select.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/util.h"

using namespace testing;
using namespace hustle::operators;
using namespace hustle;

class AggregateTestFixture : public testing::Test {
 protected:
  std::shared_ptr<arrow::Schema> schema;

  arrow::Int64Builder int_builder;
  arrow::DoubleBuilder double_builder;
  arrow::StringBuilder str_builder;
  std::shared_ptr<arrow::Array> expected_agg_col_1;
  std::shared_ptr<arrow::Array> expected_agg_col_2;
  std::shared_ptr<arrow::Array> expected_S_col_1;
  std::shared_ptr<arrow::Array> expected_S_col_2;
  std::shared_ptr<arrow::Array> expected_T_col_1;
  std::shared_ptr<arrow::Array> expected_T_col_2;

  DBTable::TablePtr R, S, T;

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
    T_csv.open("T.csv");

    for (int i = 0; i < 6; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string((5 - i) / 2) << "|";
      R_csv << std::to_string(i * 10) << std::endl;
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
 * SELECT avg(R.data) as data_mean
 * FROM R
 */
TEST_F(AggregateTestFixture, MeanTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "data"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  AggregateReference agg_ref = {AggregateKernel::MEAN, "data_mean", R, "data"};
  Aggregate agg_op(0, result, out_result, {agg_ref}, {}, {});

  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({{nullptr, "data_mean"}});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = double_builder.Append(((double)150) / 6);
  status = double_builder.Finish(&expected_agg_col_1);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 */
TEST_F(AggregateTestFixture, SumTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "data"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", R, "data"};
  Aggregate agg_op(0, result, out_result, {agg_ref}, {}, {});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({{nullptr, "data_sum"}});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.Append(150);
  status = int_builder.Finish(&expected_agg_col_1);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT count(R.data) as data_count
 * FROM R
 */
TEST_F(AggregateTestFixture, CountTest) {
    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "data"};

    auto result = std::make_shared<OperatorResult>();
    auto out_result = std::make_shared<OperatorResult>();
    result->append(R);

    AggregateReference agg_ref = {AggregateKernel::COUNT, "data_count", R, "data"};
    Aggregate agg_op(0, result, out_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(agg_op.createTask());

    scheduler.start();
    scheduler.join();

    auto out_table = out_result->materialize({{nullptr, "data_count"}});
    //    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = int_builder.Append(6);
    status = int_builder.Finish(&expected_agg_col_1);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * WHERE R.group == "R0"
 */
// TEST_F(AggregateTestFixture, SumWithSelectTest) {
//
//    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);
//
//    ColumnReference R_key_ref = {R, "key"};
//    ColumnReference R_group_ref = {R, "data"};
//
//    auto select_pred = Predicate{
//        {R, "group"},
//        arrow::compute::CompareOperator::EQUAL,
//        arrow::Datum(std::make_shared<arrow::StringScalar>("R0"))
//    };
//
//    auto select_pred_node =
//        std::make_shared<PredicateNode>(
//            std::make_shared<Predicate>(select_pred));
//
//    auto select_pred_tree = std::make_shared<PredicateTree>(select_pred_node);
//
//    auto result = std::make_shared<OperatorResult>();
//    auto out_result_select = std::make_shared<OperatorResult>();
//    auto out_result_agg = std::make_shared<OperatorResult>();
//    result->append(R);
//
//    Select select_op(0, result, out_result_select, select_pred_tree);
//
//    AggregateReference agg_ref = {AggregateKernels::SUM, "data_sum", R,
//    "data"}; Aggregate agg_op(0, out_result_select, out_result_agg, {agg_ref},
//    {}, {});
//
//    Scheduler &scheduler = Scheduler::GlobalInstance();
//
//    ExecutionPlan plan(0);
//    auto select_id = plan.addOperator(&select_op);
//    auto agg_id = plan.addOperator(&agg_op);
//
//    plan.createLink(select_id, agg_id);
//    scheduler.addTask(&plan);
//
//    scheduler.start();
//    scheduler.join();
//
//    auto out_table = out_result_agg->materialize({{nullptr, "data_sum"}});
//    out_table->print();
//
//    // Construct expected results
//    arrow::Status status;
//    status = int_builder.Append(90);
//    status = int_builder.Finish(&expected_agg_col_1);
//
//    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
//}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * GROUP BY R.group
 */
TEST_F(AggregateTestFixture, SumWithGroupByTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", R, "data"};
  Aggregate agg_op(0, result, out_result, {agg_ref}, {R_group_ref},
                   {R_group_ref});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table =
      out_result->materialize({{nullptr, "group"}, {nullptr, "data_sum"}});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = str_builder.AppendValues({"R2", "R1", "R0"});
  status = str_builder.Finish(&expected_agg_col_1);

  status = int_builder.AppendValues({10, 50, 90});
  status = int_builder.Finish(&expected_agg_col_2);

  auto group_col = std::static_pointer_cast<arrow::StringArray>(
      out_table->get_column(0)->chunk(0));
  auto agg_col = std::static_pointer_cast<arrow::Int64Array>(
      out_table->get_column(1)->chunk(0));

  for (int i = 0; i < group_col->length(); i++) {
    if (group_col->GetString(i) == "R0") {
      ASSERT_EQ(agg_col->Value(i), 90);
    } else if (group_col->GetString(i) == "R1") {
      ASSERT_EQ(agg_col->Value(i), 50);
    } else if (group_col->GetString(i) == "R2") {
      ASSERT_EQ(agg_col->Value(i), 10);
    } else {
      FAIL();
    }
  }
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * GROUP BY R.group
 * ORDER BY R.group
 */
TEST_F(AggregateTestFixture, SumWithGroupByOrderByTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", R, "data"};
  Aggregate agg_op(0, result, out_result, {agg_ref}, {R_group_ref},
                   {R_group_ref});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table =
      out_result->materialize({{nullptr, "group"}, {nullptr, "data_sum"}});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = str_builder.AppendValues({"R0", "R1", "R2"});
  status = str_builder.Finish(&expected_agg_col_1);

  status = int_builder.AppendValues({90, 50, 10});
  status = int_builder.Finish(&expected_agg_col_2);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
  EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_agg_col_2));
}

