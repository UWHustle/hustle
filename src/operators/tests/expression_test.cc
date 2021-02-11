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

#include "operators/aggregate/aggregate.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <fstream>
#include <operators/aggregate/hash_aggregate.h>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/join/join.h"
#include "operators/select/select.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/util.h"

using namespace testing;
using namespace hustle::operators;
using namespace hustle;

class ExpressionTestFixture : public testing::Test {
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
    auto field_3 = arrow::field("data1", arrow::int64());
    auto field_4 = arrow::field("data2", arrow::int64());
    auto field_5 = arrow::field("data3", arrow::int64());
    auto field_6 = arrow::field("data4", arrow::int64());

    schema = arrow::schema({field_1, field_2, field_3, field_4, 
                                                field_5, field_6});

    std::ofstream R_csv;
    R_csv.open("R.csv");

    for (int i = 0; i < 20; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string(i) << "|";
      R_csv << std::to_string(i % 10) << "|";
      R_csv << std::to_string(i * 10) << "|";
      R_csv << std::to_string(i * 5) << "|";
      R_csv << std::to_string(2) << std::endl;
    }
    R_csv.close();

  }
};

/*
 * SELECT sum(R.data2 + R.data3) as data_sum
 * FROM R
 */
TEST_F(ExpressionTestFixture, SumTest) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  std::shared_ptr<ExprReference> expr = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> lexpr = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> rexpr = std::make_shared<ExprReference>();
  
  std::shared_ptr<ColumnReference> col_ref1 = std::make_shared<ColumnReference>(ColumnReference{R, "data2"});
  std::shared_ptr<ColumnReference> col_ref2 = std::make_shared<ColumnReference>(ColumnReference{R, "data3"});

  lexpr->column_ref = col_ref1;
  rexpr->column_ref = col_ref2;

  expr->op = TK_PLUS;
  expr->left_expr = lexpr;
  expr->right_expr = rexpr;
  expr->left_expr->op = TK_AGG_COLUMN;
  expr->right_expr->op = TK_AGG_COLUMN;

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", nullptr, "", expr};
  HashAggregate agg_op(0, result, out_result, {agg_ref}, {}, {});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({{nullptr, "data_sum"}});
  //    out_table->print();

  // Construct expected results
  arrow::Status status;
  status = int_builder.Append(2850);
  status = int_builder.Finish(&expected_agg_col_1);

  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data2 + R.data3 - R.data2) as data_sum
 * FROM R
 */
TEST_F(ExpressionTestFixture, ArithmeticExpTest1) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);

  std::shared_ptr<ExprReference> lexpr = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> rexpr = std::make_shared<ExprReference>();
  
  std::shared_ptr<ColumnReference> col_ref1 = std::make_shared<ColumnReference>(ColumnReference{R, "data3"});
  std::shared_ptr<ColumnReference> col_ref2 = std::make_shared<ColumnReference>(ColumnReference{R, "data2"});

  lexpr->column_ref = col_ref1;
  lexpr->op = TK_AGG_COLUMN;
  rexpr->column_ref = col_ref2;
  rexpr->op = TK_AGG_COLUMN;
  
  std::shared_ptr<ExprReference> expr_plus = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> expr_minus = std::make_shared<ExprReference>();

  expr_minus->op = TK_MINUS;
  expr_minus->left_expr = lexpr;
  expr_minus->right_expr = rexpr;
  
  expr_plus->op = TK_PLUS;
  expr_plus->left_expr = rexpr;
  expr_plus->right_expr = expr_minus;

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", nullptr, "", expr_plus};
  HashAggregate agg_op(0, result, out_result, {agg_ref}, {}, {});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({{nullptr, "data_sum"}});

  // Construct expected results
  arrow::Status status;
  status = int_builder.Append(950);
  status = int_builder.Finish(&expected_agg_col_1);
  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}


/*
 * SELECT sum(R.data2 * R.data4) as data_sum
 * FROM R
 */
TEST_F(ExpressionTestFixture, ArithmeticExpTest2) {
  R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "data1"};

  auto result = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  result->append(R);


  std::shared_ptr<ExprReference> expr1 = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> expr2 = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> expr3 = std::make_shared<ExprReference>();

  
  std::shared_ptr<ColumnReference> col_ref1 = std::make_shared<ColumnReference>(ColumnReference{R, "data2"});
  std::shared_ptr<ColumnReference> col_ref2 = std::make_shared<ColumnReference>(ColumnReference{R, "data2"});
  std::shared_ptr<ColumnReference> col_ref3 = std::make_shared<ColumnReference>(ColumnReference{R, "data4"});

  expr1->column_ref = col_ref1;
  expr1->op = TK_AGG_COLUMN;
  expr2->column_ref = col_ref2;
  expr2->op = TK_AGG_COLUMN;
  expr3->column_ref = col_ref3;
  expr3->op = TK_AGG_COLUMN;
  
  std::shared_ptr<ExprReference> expr_minus = std::make_shared<ExprReference>();
  std::shared_ptr<ExprReference> expr_star = std::make_shared<ExprReference>();
  expr_minus->op = TK_MINUS;
  expr_minus->left_expr = expr_star;
  expr_minus->right_expr = expr2;
  
  expr_star->op = TK_STAR;
  expr_star->left_expr = expr1;
  expr_star->right_expr = expr3;

  AggregateReference agg_ref = {AggregateKernel::SUM, "data_sum", nullptr, "", expr_minus};
  HashAggregate agg_op(0, result, out_result, {agg_ref}, {}, {});
  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(agg_op.createTask());

  scheduler.start();
  scheduler.join();

  auto out_table = out_result->materialize({{nullptr, "data_sum"}});

  // Construct expected results
  arrow::Status status;
  status = int_builder.Append(1900);
  status = int_builder.Finish(&expected_agg_col_1);
  EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

