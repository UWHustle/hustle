#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"

#define BLOCK_SIZE 1024

using namespace testing;
using hustle::operators::Aggregate;
using hustle::operators::Join;
using hustle::operators::Select;

class OperatorsTestFixture : public testing::Test {
 protected:

  std::vector<std::vector<std::shared_ptr<Block>>> input_blocks;

  void SetUp() override {
    arrow::Status status;
    input_blocks = std::vector<std::vector<std::shared_ptr<Block>>>();
    // block_1_col_1
    std::shared_ptr<arrow::Array> block_1_col_1 = nullptr;
    std::shared_ptr<arrow::Field> block_1_col_1_field = arrow::field("string_id", arrow::utf8());
    arrow::StringBuilder block_1_col_1_builder = arrow::StringBuilder();
    status = block_1_col_1_builder.AppendValues({"str_A", "str_B", "str_C", "str_D", "str_E"});
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = block_1_col_1_builder.Finish(&block_1_col_1);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // block_1_col_2
    std::shared_ptr<arrow::Array> block_1_col_2 = nullptr;
    std::shared_ptr<arrow::Field> block_1_col_2_field = arrow::field("int_val", arrow::int64());
    arrow::Int64Builder block_1_col_2_builder = arrow::Int64Builder();
    status = block_1_col_2_builder.AppendValues({10, 20, 30, 75, 100}); // sum = 235
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = block_1_col_2_builder.Finish(&block_1_col_2);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // block_1 init
    auto block_1_schema = arrow::schema({block_1_col_1_field, block_1_col_2_field});
    auto block_1_record = arrow::RecordBatch::Make(block_1_schema, 5, {block_1_col_1, block_1_col_2});
    auto block_1 = std::make_shared<Block>(Block(rand(), block_1_record, BLOCK_SIZE));
    // block_2_col_1
    std::shared_ptr<arrow::Array> block_2_col_1 = nullptr;
    std::shared_ptr<arrow::Field> block_2_col_1_field = arrow::field("string_id_alt", arrow::utf8());
    arrow::StringBuilder block_2_col_1_builder = arrow::StringBuilder();
    status = block_2_col_1_builder.AppendValues({"str_A", "str_B", "str_C", "str_D", "str_E"});
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = block_2_col_1_builder.Finish(&block_2_col_1);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // block_2_col_2
    std::shared_ptr<arrow::Array> block_2_col_2 = nullptr;
    std::shared_ptr<arrow::Field> block_2_col_2_field = arrow::field("int_val", arrow::int64());
    arrow::Int64Builder block_2_col_2_builder = arrow::Int64Builder();
    status = block_2_col_2_builder.AppendValues({10, 20, 30, 75, 100});
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = block_2_col_2_builder.Finish(&block_2_col_2);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // block_2 init
    auto block_2_schema = std::make_shared<arrow::Schema>(arrow::Schema({block_2_col_1_field, block_2_col_2_field}));
    auto block_2_record = arrow::RecordBatch::Make(block_2_schema, 5, {block_2_col_1, block_2_col_2});
    auto block_2 = std::make_shared<Block>(Block(rand(), block_2_record, BLOCK_SIZE));
    // setup
    input_blocks.push_back({block_1});
    input_blocks.push_back({block_2});
  }



};

TEST_F(OperatorsTestFixture, AggregateSumTest) {
  auto *aggregate_op = new Aggregate(
      hustle::operators::AggregateKernels::SUM,
      "int_val"
  );
  std::string col_name = "int_val";
  int col_val = 235;
  //
  arrow::Status status;
  std::shared_ptr<arrow::Array> res_block_col = nullptr;
  std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name, arrow::int64());
  arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
  status = res_block_col_builder.Append(col_val);
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_builder.Finish(&res_block_col);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // res_block init
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = aggregate_op->runOperator(input_blocks);
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}

TEST_F(OperatorsTestFixture, AggregateCountTest) {
  auto *aggregate_op = new Aggregate(
      hustle::operators::AggregateKernels::COUNT,
      "int_val"
  );
  std::string col_name = "int_val";
  int col_val = 5;
  //
  //
  arrow::Status status;
  std::shared_ptr<arrow::Array> res_block_col = nullptr;
  std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name, arrow::int64());
  arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
  status = res_block_col_builder.Append(col_val);
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_builder.Finish(&res_block_col);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // res_block init
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = aggregate_op->runOperator(input_blocks);
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}

TEST_F(OperatorsTestFixture, AggregateMeanTest) {
  auto *aggregate_op = new Aggregate(
      hustle::operators::AggregateKernels::MEAN,
      "int_val"
  );
  std::string col_name = "int_val";
  int64_t col_val = 47; // 235 / 5
  //
  arrow::Status status;
  std::shared_ptr<arrow::Array> res_block_col = nullptr;
  std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name, arrow::int64());
  arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
  status = res_block_col_builder.Append(col_val);
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_builder.Finish(&res_block_col);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // res_block init
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = aggregate_op->runOperator(input_blocks);
  //TODO(nicholas) unsure why this fails.
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}

TEST_F(OperatorsTestFixture, JoinTest) {
  auto *join_op = new Join(
      "int_key"
  );
  arrow::Status status;
  // block_1_col_1
  std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
  std::shared_ptr<arrow::Field> res_block_col_1_field = arrow::field("int_key", arrow::int64());
  arrow::Int64Builder res_block_col_1_builder = arrow::Int64Builder();
  status = res_block_col_1_builder.AppendValues({10, 20, 30, 75, 100});
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_1_builder.Finish(&res_block_col_1);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // block_1_col_2
  std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
  std::shared_ptr<arrow::Field> res_block_col_2_field = arrow::field("int_val", arrow::int64());
  arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
  status = res_block_col_2_builder.AppendValues({1, 2, 3, 4, 5});
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_2_builder.Finish(&res_block_col_2);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // block_1_col_3
  std::shared_ptr<arrow::Array> res_block_col_3 = nullptr;
  std::shared_ptr<arrow::Field> res_block_col_3_field = arrow::field("int_val_alt", arrow::int64());
  arrow::Int64Builder res_block_col_3_builder = arrow::Int64Builder();
  status = res_block_col_3_builder.AppendValues({11, 22, 33, 44, 55});
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_3_builder.Finish(&res_block_col_3);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // input blocks init
  auto join_input_blocks = std::vector<std::vector<std::shared_ptr<Block>>>();
  auto join_input_block_1_schema = arrow::schema({res_block_col_1_field, res_block_col_2_field});
  auto join_input_block_1_record = arrow::RecordBatch::Make(join_input_block_1_schema, 5, {res_block_col_1, res_block_col_2});
  auto join_input_block_1 = std::make_shared<Block>(Block(rand(), join_input_block_1_record, BLOCK_SIZE));
  auto join_input_block_2_schema = arrow::schema({res_block_col_1_field, res_block_col_3_field});
  auto join_input_block_2_record = arrow::RecordBatch::Make(join_input_block_2_schema, 5, {res_block_col_1, res_block_col_3});
  auto join_input_block_2 = std::make_shared<Block>(Block(rand(), join_input_block_2_record, BLOCK_SIZE));
  join_input_blocks.push_back({join_input_block_1});
  join_input_blocks.push_back({join_input_block_2});
  // res_block init
  auto res_block_schema = arrow::schema({res_block_col_1_field, res_block_col_2_field, res_block_col_3_field});
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 5, {res_block_col_1, res_block_col_2, res_block_col_3});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = join_op->runOperator(join_input_blocks);
  //TODO(nicholas) output data values seem malformed. unsure why.
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}

TEST_F(OperatorsTestFixture, SelectTest) {
  int64_t select_val = 30;
  auto *select_op = new Select(
      arrow::compute::CompareOperator::EQUAL,
      "int_val",
      arrow::compute::Datum(select_val)
  );
  std::string col_1_name = "string_id";
  std::string col_1_val = "str_C";
  std::string col_2_name = "int_val";
  int64_t col_2_val = 30;
  //
  arrow::Status status;
  std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
  arrow::StringBuilder res_block_col_1_builder = arrow::StringBuilder();
  status = res_block_col_1_builder.AppendValues({col_1_val});
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_1_builder.Finish(&res_block_col_1);
  evaluate_status(status, __FUNCTION__, __LINE__);
  std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
  arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
  status = res_block_col_2_builder.Append(col_2_val);
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_2_builder.Finish(&res_block_col_2);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // res_block init
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({arrow::field(col_1_name, arrow::utf8()), arrow::field(col_2_name, arrow::int64())}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col_1, res_block_col_2});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = select_op->runOperator(input_blocks);
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}