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
    std::shared_ptr<Block> block_1;
    std::shared_ptr<Block> block_2;
    std::shared_ptr<Table> in_table;
    std::shared_ptr<arrow::Array> valid;

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
    block_1 = std::make_shared<Block>(0, block_1_record, BLOCK_SIZE);


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
    block_2 = std::make_shared<Block>(Block(1, block_2_record, BLOCK_SIZE));

    auto out = read_from_csv_file("../../../src/operators/tests/table_1.csv",
            block_2_schema,
            BLOCK_SIZE);


      arrow::BooleanBuilder valid_builder;
      status = valid_builder.Append(true);
      evaluate_status(status, __FUNCTION__, __LINE__);
      status = valid_builder.Finish(&valid);
      evaluate_status(status, __FUNCTION__, __LINE__);


    // setup
    input_blocks.push_back({block_1});
    input_blocks.push_back({block_2});
    two_blocks.push_back({block_1, block_2});

    in_table = std::make_shared<Table>("input",block_1_schema,BLOCK_SIZE);
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
    std::shared_ptr<arrow::Field> res_block_field= arrow::field(col_name,
            arrow::float64());
    arrow::Int64Builder res_block_col_builder = arrow::Int64Builder();
    status = res_block_col_builder.Append(col_val);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = res_block_col_builder.Finish(&res_block_col);
    evaluate_status(status, __FUNCTION__, __LINE__);
    // res_block init
    res_block_col->data()->buffers[0] = nullptr; // NOTE: Arrays
    // built from ArrayBuilder always
    // have a null bitmap.



    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema
            ({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1,
            {res_block_col});
    auto res_block = std::make_shared<Block>(Block(0, res_block_record,
            BLOCK_SIZE));
    // result/out compare

    in_table->insert_blocks({block_1});
    auto out_table = aggregate_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals(*res_block->get_records()));
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
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema
                                                                    ({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1,
            {res_block_col});
    auto res_block = std::make_shared<Block>(Block(0, res_block_record,
                                                   BLOCK_SIZE));
    // result/out compare

    in_table->insert_blocks({block_1, block_2});
    auto out_table = aggregate_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals(*res_block->get_records()));
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
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema
                                                                    ({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1,
                                                     {res_block_col});
    auto res_block = std::make_shared<Block>(Block(0, res_block_record,
                                                   BLOCK_SIZE));
    in_table->insert_blocks({block_1});
    auto out_table = aggregate_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals(*res_block->get_records()));

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
    auto res_block_schema = std::make_shared<arrow::Schema>(arrow::Schema
                                                                    ({res_block_field}));
    auto res_block_record = arrow::RecordBatch::Make(res_block_schema, 1,
                                                     {res_block_col});
    auto res_block = std::make_shared<Block>(Block(0, res_block_record,
                                                   BLOCK_SIZE));
    in_table->insert_blocks({block_1, block_2});
    auto out_table = aggregate_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals(*res_block->get_records()));
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

    in_table->insert_blocks({block_1});
    // result/out compare
    auto out_table = select_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals
    (*res_block->get_records()));

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


    in_table->insert_blocks({block_1});
    // result/out compare
    auto out_table = select_op->runOperator({in_table});
    EXPECT_TRUE(out_table->get_block(0)->get_records()->Equals
            (*res_block->get_records()));
}




class OperatorsTestFixture2 : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<arrow::Schema> schema_2;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<Table> in_left_table;
    std::shared_ptr<Table> in_right_table;

    void SetUp() override {
        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("id",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
        std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
        std::shared_ptr<arrow::Field> field4 = arrow::field("D",
                                                            arrow::int64());
        schema = arrow::schema({field1, field2, field3, field4});
        schema_2 = arrow::schema({field1, field2});

        std::ofstream left_table_csv;
        left_table_csv.open("left_table.csv");
        for (int i = 0; i < 9; i++) {

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
        for (int i = 0; i < 9; i++) {
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


        std::ofstream left_table_csv_2;
        left_table_csv_2.open("left_table_2.csv");
        for (int i = 0; i < 100; i++) {
            left_table_csv_2 << std::to_string(i) + "|My key is " +
                                std::to_string(i) + "\n";
        }
        left_table_csv_2.close();

        std::ofstream right_table_csv_2;
        right_table_csv_2.open("right_table_2.csv");
        for (int i = 0; i < 100; i++) {
            right_table_csv_2 << std::to_string(i) + "|And my key is also " +
            std::to_string(i)+ "\n";
        }
        right_table_csv_2.close();


    }
};

TEST_F(OperatorsTestFixture2, SelectFromCSV) {

    in_left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto *select_op = new hustle::operators::Select(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );

    auto out_table = select_op->runOperator({in_left_table});

    for (int i=0; i<out_table->get_num_blocks(); i++) {
        auto block = out_table->get_block(i);

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_valid_column());
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(0));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(1));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));
        column4 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(3));

        for (int row = 0; row < block->get_num_rows(); row++) {
            EXPECT_EQ(valid->Value(row), true);
            EXPECT_EQ(column1->Value(row), 1776);
            EXPECT_EQ(column2->GetString(row),
                      "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(row),
                      "Twice two makes five is sometimes a very charming "
                      "thing too.");
            EXPECT_EQ(column4->Value(row), 0);
        }
    }
}


TEST_F(OperatorsTestFixture2, SelectFromCSVTwoConditionsSame) {

    in_left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto left_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );
    auto right_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );

    auto composite_select_op = new hustle::operators::SelectComposite(
            left_select_op, right_select_op,
            hustle::operators::FilterOperator::AND
            );

    auto out_table = composite_select_op->runOperator({in_left_table});

    for (int i=0; i<out_table->get_num_blocks(); i++) {
        auto block = out_table->get_block(i);

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_valid_column());
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(0));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(1));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));
        column4 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(3));

        for (int row = 0; row < block->get_num_rows(); row++) {
            EXPECT_EQ(valid->Value(row), true);
            EXPECT_EQ(column1->Value(row), 1776);
            EXPECT_EQ(column2->GetString(row),
                      "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(row),
                      "Twice two makes five is sometimes a very charming "
                      "thing too.");
            EXPECT_EQ(column4->Value(row), 0);
        }
    }
}


TEST_F(OperatorsTestFixture2, SelectFromCSVTwoConditionsDifferent) {

    in_left_table = read_from_csv_file
            ("left_table_2.csv", schema_2, BLOCK_SIZE);

    auto left_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 42)
    );


    std::shared_ptr<arrow::Scalar> string =
            std::make_shared<arrow::StringScalar>("My key is 42");

    auto k = string->type->name();

    auto right_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "B",
            arrow::compute::Datum(string)
    );

    auto composite_select_op = new hustle::operators::SelectComposite(
            left_select_op, right_select_op,
            hustle::operators::FilterOperator::AND
    );

    auto out_table = composite_select_op->runOperator({in_left_table});

    auto block = out_table->get_block(0);

    valid = std::static_pointer_cast<arrow::BooleanArray>
            (block->get_valid_column());
    column1 = std::static_pointer_cast<arrow::Int64Array>
            (block->get_column(0));
    column2 = std::static_pointer_cast<arrow::StringArray>
            (block->get_column(1));

    EXPECT_EQ(valid->Value(0), true);
    EXPECT_EQ(column1->Value(0), 42);
    EXPECT_EQ(column2->GetString(0),"My key is 42");

}


TEST_F(OperatorsTestFixture2, HashJoin) {

    auto left_table = read_from_csv_file
            ("left_table_2.csv", schema_2, BLOCK_SIZE);

    auto right_table = read_from_csv_file
            ("right_table_2.csv", schema_2, BLOCK_SIZE);

    auto join_op = hustle::operators::Join("id","id");

    auto out_table = join_op.hash_join(left_table, right_table);

    for (int i=0; i<out_table->get_num_blocks(); i++) {

        auto block = out_table->get_block(i);

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_valid_column());
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(0));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(1));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));

        int table_row = 0;

        for (int block_row = 0; block_row < block->get_num_rows(); block_row++) {

            table_row = block_row + out_table->get_block_row_offset(i);

            EXPECT_EQ(valid->Value(block_row), true);
            EXPECT_EQ(column1->Value(block_row), table_row);
            EXPECT_EQ(column2->GetString(block_row),
                      "My key is " + std::to_string(table_row));
            EXPECT_EQ(column3->GetString(block_row),
                      "And my key is also " + std::to_string(table_row));
        }
    }
}

TEST_F(OperatorsTestFixture2, HashJoinEmptyResult) {

    auto left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto right_table = read_from_csv_file
            ("right_table.csv", schema, BLOCK_SIZE);

    auto join_op = hustle::operators::Join("D", "D");

    auto out_table = join_op.hash_join(left_table, right_table);

    EXPECT_EQ(out_table->get_num_rows(), 0);
    EXPECT_EQ(out_table->get_num_blocks(), 0);
}




class SSBTestFixture : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> lineorder_schema;
    std::shared_ptr<arrow::Schema> date_schema;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<Table> lineorder;
    std::shared_ptr<Table> date;

    void SetUp() override {

        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("order key",
                arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("line number",
                arrow::int64());
        std::shared_ptr<arrow::Field> field3 = arrow::field("cust key",
                arrow::int64());
        std::shared_ptr<arrow::Field> field4 = arrow::field("part key",
                arrow::int64());
        std::shared_ptr<arrow::Field> field5 = arrow::field("supp key",
                arrow::int64());
        std::shared_ptr<arrow::Field> field6 = arrow::field("order date",
                arrow::int64());
        std::shared_ptr<arrow::Field> field7 = arrow::field("ord priority",
                arrow::utf8());
        std::shared_ptr<arrow::Field> field8 = arrow::field("ship priority",
                arrow::utf8());
        std::shared_ptr<arrow::Field> field9 = arrow::field("quantity",
                arrow::int64());
        std::shared_ptr<arrow::Field> field10 = arrow::field("extended price",
                arrow::int64());
        std::shared_ptr<arrow::Field> field11 = arrow::field("ord total price",
                arrow::int64());
        std::shared_ptr<arrow::Field> field12 = arrow::field("discount",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field13 = arrow::field("revenue",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field14 = arrow::field("supply cost",
                arrow::int64());
        std::shared_ptr<arrow::Field> field15 = arrow::field("tax",
                arrow::int64());
        std::shared_ptr<arrow::Field> field16 = arrow::field("commit date",
                arrow::int64());
        std::shared_ptr<arrow::Field> field17 = arrow::field("ship mode",
                arrow::utf8());
        lineorder_schema = arrow::schema({field1, field2, field3, field4,
                                          field5,
                                field6, field7, field8, field9, field10,
                                field11, field12, field13, field14, field15,
                                field16, field17});


        std::shared_ptr<arrow::Field> d_field1 = arrow::field("date key",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> d_field2 = arrow::field("date",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> d_field3 = arrow::field("day of week",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> d_field4 = arrow::field("month",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> d_field5 = arrow::field("year",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> d_field6 = arrow::field("year month num",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> d_field7 = arrow::field("year month",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> d_field8 = arrow::field("day num in week",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> d_field9 = arrow::field("day num in "
                                                              "month",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> d_field10 = arrow::field("day num in "
                                                               "year",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field11 = arrow::field("month num in "
                                                               "year",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field12 = arrow::field("week num in "
                                                               "year",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field13 = arrow::field("selling season",
                                                             arrow::utf8());
        std::shared_ptr<arrow::Field> d_field14 = arrow::field("last day in "
                                                               "week fl",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field15 = arrow::field("last day in "
                                                               "month fl",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field16 = arrow::field("holiday fl",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> d_field17 = arrow::field("weekday fl",
                                                             arrow::int64());

        date_schema = arrow::schema({ d_field1,  d_field2,  d_field3,  d_field4,  d_field5,
                                 d_field6,  d_field7,  d_field8,  d_field9,  d_field10,
                                 d_field11,  d_field12,  d_field13,  d_field14,  d_field15,
                                 d_field16,  d_field17});
    }
};

TEST_F(SSBTestFixture, SSBQ1) {

    // NOTE: I forgot to include the selection Lineorder.lo_quantity < 25

    lineorder = read_from_csv_file
            ("/Users/corrado/hustle/cmake-build-debug/src/operators/lineorder_small.tbl", lineorder_schema, BLOCK_SIZE);

    date = read_from_csv_file
            ("/Users/corrado/hustle/cmake-build-debug/src/operators/date.tbl", date_schema, BLOCK_SIZE);

    // Create select operator for Date.year = 1993
    auto date_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1993)
    );

    // Create select operator for Lineorder.discount >= 1
    auto lineorder_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 1)
    );

    // Create select operator for Lineorder.discount <= 3
    auto lineorder_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 3)
    );

    // Combine select operators for lineorder into one composite operator.
    auto lineorder_select_op_composite =
            std::make_shared<hustle::operators::SelectComposite>
                    (lineorder_select_op_1, lineorder_select_op_2,
                     hustle::operators::FilterOperator::AND);

    // Create natural join operator for left.order date == right.date key
    // For this query, left corresponds to Lineorder, and right corresponds
    // to Date.
    auto join_op = std::make_shared<hustle::operators::Join>("order date",
            "date key");

    // Perform selection on Lineorder
    auto lineorder_2 = lineorder_select_op_composite->runOperator({lineorder});
    // Perform selection on Date
    auto date_2 = date_select_op->runOperator({date});

    // Join the resulting Lineorder and Date tables
    auto join_table = join_op->hash_join(lineorder_2, date_2);

    // Create aggregate operator
    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>
            (hustle::operators::AggregateKernels::SUM, "revenue");

    // Perform aggregate over resulting join table
    auto aggregate = aggregate_op->runOperator({join_table});

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();
}