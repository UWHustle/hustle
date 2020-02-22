#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"


#include <arrow/compute/kernels/filter.h>
#include <fstream>

#define BLOCK_SIZE 1024

using namespace testing;
using hustle::operators::Aggregate;
using hustle::operators::Join;
using hustle::operators::Select;

class OperatorsTestFixture : public testing::Test {
 protected:

  std::vector<std::vector<std::shared_ptr<Block>>> input_blocks;
    std::vector<std::vector<std::shared_ptr<Block>>> two_blocks;

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
    status = block_1_col_2_builder.AppendValues({10, 30, 30, 65, 100}); //
    // sum = 235
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
    status = block_2_col_2_builder.AppendValues({0, 30, 30, 65, 110});
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
    two_blocks.push_back({block_1, block_2});
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
  res_block_col->data()->buffers[0] = nullptr; // NOTE: Arrays
  // built from ArrayBuilder always
  // have a null bitmap.
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = aggregate_op->runOperator(input_blocks);
  EXPECT_TRUE(out_block[0]->get_records()->Equals(*res_block->get_records()));
    auto res = res_block->get_records()->column(0)->data();
    auto out = out_block[0]->get_records()->column(0)->data();
    int x=0;
}

TEST_F(OperatorsTestFixture, AggregateSumTestTwoBlocks) {
    auto *aggregate_op = new Aggregate(
            hustle::operators::AggregateKernels::SUM,
            "int_val"
    );
    std::string col_name = "int_val";
    int col_val = 235*2;
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
    res_block_col->data()->buffers[0] = nullptr; // NOTE: Arrays
    // built from ArrayBuilder always
    // have a null bitmap.
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
    // result/out compare
    auto out_block = aggregate_op->runOperator(two_blocks);
    EXPECT_TRUE(out_block[0]->get_records()->Equals(*res_block->get_records()));
    auto res = res_block->get_records()->column(0)->data();
    auto out = out_block[0]->get_records()->column(0)->data();
    int x=0;
}

//TEST_F(OperatorsTestFixture, AggregateCountTest) {
//  auto *aggregate_op = new Aggregate(
//      hustle::operators::AggregateKernels::COUNT,
//      "int_val"
//  );
//  std::string col_name = "int_val";
//  int col_val = 5;
//  //
//  //
//  arrow::Status status;
//  std::shared_ptr<arrow::Array> res_block_col = nullptr;
//  std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name, arrow::int64());
//  arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
//  status = res_block_col_builder.Append(col_val);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_builder.Finish(&res_block_col);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  // res_block init
//  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
//  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
//  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
//  // result/out compare
//  auto out_block = aggregate_op->runOperator(input_blocks);
//  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
//}
//
//TEST_F(OperatorsTestFixture, AggregateCountTwoBlocks) {
//    auto *aggregate_op = new Aggregate(
//            hustle::operators::AggregateKernels::COUNT,
//            "int_val"
//    );
//    std::string col_name = "int_val";
//    int col_val = 5*2;
//    //
//    //
//    arrow::Status status;
//    std::shared_ptr<arrow::Array> res_block_col = nullptr;
//    std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name, arrow::int64());
//    arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
//    status = res_block_col_builder.Append(col_val);
//    evaluate_status(status, __FUNCTION__, __LINE__);
//    status = res_block_col_builder.Finish(&res_block_col);
//    evaluate_status(status, __FUNCTION__, __LINE__);
//    // res_block init
//    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
//    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
//    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
//    // result/out compare
//    auto out_block = aggregate_op->runOperator(two_blocks);
//    EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
//}

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
  std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name,
          arrow::float64());
  arrow::DoubleBuilder res_block_col_builder = arrow::DoubleBuilder();
  status = res_block_col_builder.Append(col_val);
  evaluate_status(status, __FUNCTION__, __LINE__);
  status = res_block_col_builder.Finish(&res_block_col);
  evaluate_status(status, __FUNCTION__, __LINE__);
  // res_block init
  res_block_col->data()->buffers[0] =nullptr;
  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
  // result/out compare
  auto out_block = aggregate_op->runOperator(input_blocks);
  //TODO(nicholas) unsure why this fails.
  // because the mean is not being correclty outputted. out_block[0] does not
  // actually contain the mean.
  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}

TEST_F(OperatorsTestFixture, AggregateMeanTwoBlocks) {
    auto *aggregate_op = new Aggregate(
            hustle::operators::AggregateKernels::MEAN,
            "int_val"
    );
    std::string col_name = "int_val";
    int64_t col_val = 47; // 235 / 5
    //
    arrow::Status status;
    std::shared_ptr<arrow::Array> res_block_col = nullptr;
    std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name,
                                                                arrow::float64());
    arrow::DoubleBuilder res_block_col_builder = arrow::DoubleBuilder();
    status = res_block_col_builder.Append(col_val);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_builder.Finish(&res_block_col);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // res_block init
    res_block_col->data()->buffers[0] =nullptr;
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1, {res_block_col});
    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
    // result/out compare
    auto out_block = aggregate_op->runOperator(two_blocks);
    //TODO(nicholas) unsure why this fails.
    // because the mean is not being correclty outputted. out_block[0] does not
    // actually contain the mean.
    EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
}
//
//TEST_F(OperatorsTestFixture, JoinTest) {
//  auto *join_op = new Join(
//      "int_key"
//  );
//  arrow::Status status;
//  // block_1_col_1
//  std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
//  std::shared_ptr<arrow::Field> res_block_col_1_field = arrow::field("int_key", arrow::int64());
//  arrow::Int64Builder res_block_col_1_builder = arrow::Int64Builder();
//  status = res_block_col_1_builder.AppendValues({10, 20, 30, 75, 100});
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_1_builder.Finish(&res_block_col_1);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//  // block_1_col_2
//  std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
//  std::shared_ptr<arrow::Field> res_block_col_2_field = arrow::field("int_val", arrow::int64());
//  arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
//  status = res_block_col_2_builder.AppendValues({1, 2, 3, 4, 5});
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_2_builder.Finish(&res_block_col_2);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//  // block_1_col_3
//  std::shared_ptr<arrow::Array> res_block_col_3 = nullptr;
//  std::shared_ptr<arrow::Field> res_block_col_3_field = arrow::field("int_val_alt", arrow::int64());
//  arrow::Int64Builder res_block_col_3_builder = arrow::Int64Builder();
//  status = res_block_col_3_builder.AppendValues({11, 22, 33, 44, 55});
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_3_builder.Finish(&res_block_col_3);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//  // input blocks init
//  auto join_input_blocks = std::vector<std::vector<std::shared_ptr<Block>>>();
//
//  auto join_input_block_1_schema = arrow::schema({res_block_col_1_field, res_block_col_2_field});
//  auto join_input_block_1_record = arrow::RecordBatch::Make(join_input_block_1_schema, 5, {res_block_col_1, res_block_col_2});
//  auto join_input_block_1 = std::make_shared<Block>(Block(rand(), join_input_block_1_record, BLOCK_SIZE));
//
//  auto join_input_block_2_schema = arrow::schema({res_block_col_1_field, res_block_col_3_field});
//  auto join_input_block_2_record = arrow::RecordBatch::Make(join_input_block_2_schema, 5, {res_block_col_1, res_block_col_3});
//  auto join_input_block_2 = std::make_shared<Block>(Block(rand(), join_input_block_2_record, BLOCK_SIZE));
//
//  join_input_blocks.push_back({join_input_block_1});
//  join_input_blocks.push_back({join_input_block_2});
//
//
//  // res_block init
//  auto res_block_schema = arrow::schema({res_block_col_1_field, res_block_col_2_field, res_block_col_3_field});
//  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 5, {res_block_col_1, res_block_col_2, res_block_col_3});
//  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
//
//
//  // result/out compare
//  auto out_block = join_op->runOperator(join_input_blocks);
//  //TODO(nicholas) output data values seem malformed. unsure why.
//  EXPECT_TRUE(out_block[0]->get_records()->ApproxEquals(*res_block->get_records()));
//
//    res_block->print();
//    out_block[0]->print();
//
//    auto res = res_block->get_records()->column(0)->data();
//    auto out1 = out_block[0]->get_records()->column(0)->data();
//    auto out2 = out_block[0]->get_records()->column(1)->data();
//    auto out3 = out_block[0]->get_records()->column(2)->data();
//    int x = 0;
//}
//
//TEST_F(OperatorsTestFixture, SelectOneBlock) {
//  int64_t select_val = 30;
//  auto *select_op = new Select(
//      arrow::compute::CompareOperator::EQUAL,
//      "int_val",
//      arrow::compute::Datum(select_val)
//  );
//  std::string col_1_name = "string_id";
//  std::string col_2_name = "int_val";
//
//
//  arrow::Status status;
//
//
//  std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
//  arrow::StringBuilder res_block_col_1_builder = arrow::StringBuilder();
//  status = res_block_col_1_builder.AppendValues({"str_B","str_C"});
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_1_builder.Finish(&res_block_col_1);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//  std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
//  arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
//  status = res_block_col_2_builder.Append(30);
//  status = res_block_col_2_builder.Append(30);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//  status = res_block_col_2_builder.Finish(&res_block_col_2);
//  evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//  // res_block init
//  auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({arrow::field(col_1_name, arrow::utf8()), arrow::field(col_2_name, arrow::int64())}));
//  auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 2,
//          {res_block_col_1, res_block_col_2});
//  auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
//
//
//  // result/out compare
//  auto out_block = select_op->runOperator(input_blocks);
//  EXPECT_TRUE(out_block[0]->get_records()->Equals(*res_block->get_records()));
//
//  std::cout << std::endl;
//  out_block[0]->print();
//  res_block->print();
//}
//
//TEST_F(OperatorsTestFixture, SelectOneBlockEmpty) {
//    int64_t select_val = 3000000;
//    auto *select_op = new Select(
//            arrow::compute::CompareOperator::EQUAL,
//            "int_val",
//            arrow::compute::Datum(select_val)
//    );
//    std::string col_1_name = "string_id";
//    std::string col_2_name = "int_val";
//
//    arrow::Status status;
//
//    std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
//    arrow::StringBuilder res_block_col_1_builder = arrow::StringBuilder();
//    evaluate_status(status, __FUNCTION__, __LINE__);
//    status = res_block_col_1_builder.Finish(&res_block_col_1);
//    evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//    std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
//    arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
//    evaluate_status(status, __FUNCTION__, __LINE__);
//    status = res_block_col_2_builder.Finish(&res_block_col_2);
//    evaluate_status(status, __FUNCTION__, __LINE__);
//
//
//    // res_block init
//    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({arrow::field(col_1_name, arrow::utf8()), arrow::field(col_2_name, arrow::int64())}));
//    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 0,
//                                                     {res_block_col_1, res_block_col_2});
//    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
//
//    // result/out compare
//    auto out_block = select_op->runOperator(input_blocks);
//    EXPECT_TRUE(out_block[0]->get_records()->Equals(*res_block->get_records()));
//
//    std::cout << std::endl;
//    out_block[0]->print();
//    res_block->print();
//}


TEST_F(OperatorsTestFixture, SelectTwoBlocks) {
    int64_t select_val = 30;
    auto *select_op = new Select(
            arrow::compute::CompareOperator::EQUAL,
            "int_val",
            arrow::compute::Datum(select_val)
    );
    std::string col_1_name = "string_id";
    std::string col_2_name = "int_val";


    arrow::Status status;


    std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
    arrow::StringBuilder res_block_col_1_builder = arrow::StringBuilder();
    status = res_block_col_1_builder.AppendValues({"str_B","str_C"});
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_1_builder.Finish(&res_block_col_1);
    evaluate_status(status, __FUNCTION__, __LINE__);


    std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
    arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
    status = res_block_col_2_builder.Append(30);
    status = res_block_col_2_builder.Append(30);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_2_builder.Finish(&res_block_col_2);
    evaluate_status(status, __FUNCTION__, __LINE__);


    // res_block init
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({arrow::field(col_1_name, arrow::utf8()), arrow::field(col_2_name, arrow::int64())}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 2,
                                                     {res_block_col_1, res_block_col_2});
    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
    auto res_block_2 = std::make_shared<Block>(Block(rand(), res_block_record,
            BLOCK_SIZE));


    // result/out compare
    auto out_block = select_op->runOperator(two_blocks);
    EXPECT_TRUE(out_block[0]->get_records()->Equals(*res_block->get_records()));
    EXPECT_TRUE(out_block[1]->get_records()->Equals(*res_block_2->get_records()));

}



TEST_F(OperatorsTestFixture, SelectTwoBlocksOneEmpty) {
    int64_t select_val = 10;
    auto *select_op = new Select(
            arrow::compute::CompareOperator::EQUAL,
            "int_val",
            arrow::compute::Datum(select_val)
    );
    std::string col_1_name = "string_id";
    std::string col_2_name = "int_val";

    arrow::Status status;

    std::shared_ptr<arrow::Array> res_block_col_1 = nullptr;
    arrow::StringBuilder res_block_col_1_builder = arrow::StringBuilder();
    status = res_block_col_1_builder.AppendValues({"str_A"});
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_1_builder.Finish(&res_block_col_1);
    evaluate_status(status, __FUNCTION__, __LINE__);


    std::shared_ptr<arrow::Array> res_block_col_2 = nullptr;
    arrow::Int64Builder res_block_col_2_builder = arrow::Int64Builder();
    status = res_block_col_2_builder.Append(10);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_2_builder.Finish(&res_block_col_2);
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::Array> arr1;
    status = res_block_col_1_builder.Finish(&arr1);
    evaluate_status(status, __FUNCTION__, __LINE__);
    std::shared_ptr<arrow::Array> arr2;
    status = res_block_col_2_builder.Finish(&arr2);
    evaluate_status(status, __FUNCTION__, __LINE__);

    // res_block init
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema({arrow::field(col_1_name, arrow::utf8()), arrow::field(col_2_name, arrow::int64())}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1,
                                                     {res_block_col_1, res_block_col_2});
    auto res_block_record_2 = arrow::RecordBatch::Make(res_block_schema, 0,
                                                     {arr1,
                                                      arr2});
    auto res_block = std::make_shared<Block>(Block(rand(), res_block_record, BLOCK_SIZE));
    auto res_block_2 = std::make_shared<Block>(Block(rand(), res_block_record_2,
                                                     BLOCK_SIZE));


    // result/out compare
    auto out_block = select_op->runOperator(two_blocks);
    EXPECT_TRUE(out_block[0]->get_records()->Equals
    (*res_block->get_records()));
    EXPECT_TRUE(out_block[1]->get_records()->Equals
    (*res_block_2->get_records()));
}




class OperatorsTestFixture2 : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    void SetUp() override {
        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("A",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
        std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
        std::shared_ptr<arrow::Field> field4 = arrow::field("D",
                                                            arrow::int64());
        schema = arrow::schema(
                {field1, field2, field3, field4});

        std::ofstream left_table_csv;
        left_table_csv.open("left_table.csv");
        for (int i = 0; i < 8; i++) {

            left_table_csv<< std::to_string(i) + "|Mon dessin ne representait"
                                                 "  pas un chapeau.|Il "
                                                 "representait un serpent boa qui digerait un elephant"
                                                 ".|0\n";
            left_table_csv << "1776|Twice two makes four is an excellent thing"
                        ".|Twice two makes five is sometimes a very charming "
                        "thing too.|0\n";
        }
        left_table_csv.close();


        std::ofstream right_table_csv;
        right_table_csv.open("right_table.csv");
        for (int i = 0; i < 8; i++) {
            right_table_csv<< "4242|Mon dessin ne representait pas un chapeau"
                            ".|Il "
                             "representait un serpent boa qui digerait un elephant"
                             ".|37373737\n";
            right_table_csv << std::to_string(i) + "|Twice two makes four is "
                                                   "an excellent thing"
                              ".|Twice two makes five is sometimes a very charming "
                              "thing too.|1789\n";
        }
        right_table_csv.close();
    }
};

TEST_F(OperatorsTestFixture2, SelectFromCSV) {

    auto table_from_csv = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    std::vector<std::shared_ptr<Block>> blocks;
    for (int i=0; i<table_from_csv->get_num_blocks(); i++){
        blocks.push_back(table_from_csv->get_block(i));
    }

    auto *select_op = new hustle::operators::Select(
            arrow::compute::CompareOperator::EQUAL,
            "A",
            arrow::compute::Datum((int64_t) 1776)
    );

    auto out_blocks = select_op->runOperator({blocks});

    for (auto block : out_blocks) {

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_column(0));
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(1));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(3));
        column4 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(4));

        for (int row = 0; row < block->get_num_rows(); row++) {
            EXPECT_EQ(valid->Value(row), true);
            EXPECT_EQ(column1->Value(row), 1776);
            EXPECT_EQ(column2->GetString(row),
                      "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(row),
                      "Twice two makes five is sometimes a very charming "
                      "thing too.");
            EXPECT_EQ(column4->Value(row), 1789);
        }
    }
}

TEST_F(OperatorsTestFixture2, HashJoin) {

    auto left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto right_table = read_from_csv_file
            ("right_table.csv", schema, BLOCK_SIZE);

    auto join_op = hustle::operators::Join("A");

    auto out_table = join_op.hash_join(left_table, right_table);

    out_table->print();
//    for (auto block : out_table->) {
//
//        valid = std::static_pointer_cast<arrow::BooleanArray>
//                (block->get_column(0));
//        column1 = std::static_pointer_cast<arrow::Int64Array>
//                (block->get_column(1));
//        column2 = std::static_pointer_cast<arrow::StringArray>
//                (block->get_column(2));
//        column3 = std::static_pointer_cast<arrow::StringArray>
//                (block->get_column(3));
//        column4 = std::static_pointer_cast<arrow::Int64Array>
//                (block->get_column(4));
//
//        for (int row = 0; row < block->get_num_rows(); row++) {
//            EXPECT_EQ(valid->Value(row), true);
//            EXPECT_EQ(column1->Value(row), 1776);
//            EXPECT_EQ(column2->GetString(row),
//                      "Twice two makes four is an excellent thing.");
//            EXPECT_EQ(column3->GetString(row),
//                      "Twice two makes five is sometimes a very charming "
//                      "thing too.");
//            EXPECT_EQ(column4->Value(row), 1789);
//        }
//    }
}

TEST_F(OperatorsTestFixture2, HashJoinEmptyResult) {

    auto left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto right_table = read_from_csv_file
            ("right_table.csv", schema, BLOCK_SIZE);

    auto join_op = hustle::operators::Join("D");

    auto out_table = join_op.hash_join(left_table, right_table);

    out_table->print();
}