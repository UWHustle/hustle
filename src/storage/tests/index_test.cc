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

#include <exception>
#include <fstream>
#include <stdexcept>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/select/select.h"
#include "scheduler/scheduler.h"
#include "storage/base_block.h"
#include "storage/base_table.h"
#include "storage/index_aware_table.h"
#include "storage/utils/util.h"

#define BLOCK_SIZE 108

using namespace testing;
using namespace hustle;
using namespace hustle::operators;

class IndexTestFixture
    : public testing::TestWithParam<
          std::tuple<arrow::compute::CompareOperator, arrow::Datum>> {
 protected:
  std::shared_ptr<BaseTable> R;
  std::shared_ptr<arrow::Schema> schema_R;
  std::shared_ptr<IndexAwareTable> R_index;
  std::shared_ptr<arrow::Schema> schema_R_index;

  void SetUp() override {
    arrow::Status status;
    auto field_1 = arrow::field("key", arrow::int64());
    auto field_2 = arrow::field("group", arrow::utf8());
    auto field_3 = arrow::field("data", arrow::int64());
    schema_R = arrow::schema({field_1, field_2, field_3});
    schema_R_index = arrow::schema({field_1, field_2, field_3});

    std::ofstream R_csv;
    R_csv.open("R.csv");
    for (int i = 0; i < 6; i++) {
      R_csv << std::to_string(i) << "|";
      R_csv << "R" << std::to_string(i / 2) << "|";
      R_csv << std::to_string(i * 10) << std::endl;
    }
    R_csv.close();
  }
};

TEST_P(IndexTestFixture, ParameterizedSelectComparisonTest) {
  arrow::compute::CompareOperator predicate_op = std::get<0>(GetParam());
  arrow::Datum predicate_val = std::get<1>(GetParam());
  ColumnReference R_key_ref = {R, "key"};
  ColumnReference R_group_ref = {R, "group"};
  ColumnReference R_data_ref = {R, "data"};

  R = std::dynamic_pointer_cast<BaseTable>(
      read_from_csv_file("R.csv", schema_R, BLOCK_SIZE, false));
  auto select_predicate_R = Predicate{{R, "data"}, predicate_op, predicate_val};
  auto select_predicate_node_R = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_predicate_R));
  auto select_predicate_tree_R =
      std::make_shared<PredicateTree>(select_predicate_node_R);
  auto in_result_R = std::make_shared<OperatorResult>();
  auto out_result_R = std::make_shared<OperatorResult>();
  in_result_R->append(R);
  Select select_op_R(0, R, in_result_R, out_result_R, select_predicate_tree_R);

  R_index = std::dynamic_pointer_cast<IndexAwareTable>(
      read_from_csv_file("R.csv", schema_R_index, BLOCK_SIZE, true));
  auto select_predicate_R_index =
      Predicate{{R_index, "data"}, predicate_op, predicate_val};
  auto select_predicate_node_R_index = std::make_shared<PredicateNode>(
      std::make_shared<Predicate>(select_predicate_R_index));
  auto select_predicate_tree_R_index =
      std::make_shared<PredicateTree>(select_predicate_node_R_index);
  auto in_result_R_index = std::make_shared<OperatorResult>();
  auto out_result_R_index = std::make_shared<OperatorResult>();
  in_result_R_index->append(R_index);
  Select select_op_R_index(0, R_index, in_result_R_index, out_result_R_index,
                           select_predicate_tree_R);
  Scheduler& scheduler = Scheduler::GlobalInstance();
  scheduler.addTask(select_op_R.createTask());
  scheduler.addTask(select_op_R_index.createTask());
  scheduler.start();
  scheduler.join();
  auto out_table_R =
      out_result_R->materialize({R_key_ref, R_group_ref, R_data_ref}, false);
  auto out_table_R_index = out_result_R_index->materialize(
      {R_key_ref, R_group_ref, R_data_ref}, true);
  EXPECT_EQ(R_index->GetIndexStatusList(0).size(),
            1);  // column 0, int64, 1 SMA metadata
  EXPECT_EQ(R_index->GetIndexStatusList(0)[0],
            arrow::Status::OK());  // validate metadata ok
  EXPECT_EQ(R_index->GetIndexStatusList(1).size(),
            0);  // column 1, utf8, no metadata
  // column 1, no metadata to validate
  EXPECT_EQ(R_index->GetIndexStatusList(2).size(),
            1);  // column 2, int64, 1 SMA metadata
  EXPECT_EQ(R_index->GetIndexStatusList(2)[0],
            arrow::Status::OK());  // validate metadata ok
  EXPECT_TRUE(std::dynamic_pointer_cast<IndexAwareTable>(out_table_R) ==
              nullptr);
  EXPECT_TRUE(std::dynamic_pointer_cast<IndexAwareTable>(out_table_R_index) !=
              nullptr);
  EXPECT_TRUE(out_table_R->get_column(0)->chunk(0)->Equals(
      out_table_R_index->get_column(0)->chunk(0)));
  EXPECT_TRUE(out_table_R->get_column(1)->chunk(0)->Equals(
      out_table_R_index->get_column(1)->chunk(0)));
  EXPECT_TRUE(out_table_R->get_column(2)->chunk(0)->Equals(
      out_table_R_index->get_column(2)->chunk(0)));
}

INSTANTIATE_TEST_SUITE_P(
    TestMetadataSelectEquivalance, IndexTestFixture,
    ::testing::Values(
        std::make_tuple(arrow::compute::CompareOperator::GREATER,
                        arrow::Datum((int64_t)30)),
        std::make_tuple(arrow::compute::CompareOperator::GREATER_EQUAL,
                        arrow::Datum((int64_t)30)),
        std::make_tuple(arrow::compute::CompareOperator::LESS,
                        arrow::Datum((int64_t)30)),
        std::make_tuple(arrow::compute::CompareOperator::LESS_EQUAL,
                        arrow::Datum((int64_t)30)),
        std::make_tuple(arrow::compute::CompareOperator::EQUAL,
                        arrow::Datum((int64_t)30))
        // not implemented until NOT_EQUAL no longer implements BETWEEN operator
        // behavior.
        // std::make_tuple(arrow::compute::CompareOperator::NOT_EQUAL,
        //                 arrow::Datum((int64_t)30))
        ));
