#include <arrow/io/api.h>

#include <filesystem>
#include <iostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "table/table.h"
#include "table/util.h"

#define BLOCK_SIZE \
  1024  // Force the block size to 1 KB for the sake of this test

using namespace testing;

class HustleBlockTest : public testing::Test {
 protected:
  void SetUp() override {
    std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
    std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
    std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
    std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
    schema = arrow::schema({field1, field2, field3, field4});

    uint64_t f1, f4;

    // Create record_1
    record_string_1 =
        "00000000Mon dessin ne representait pas un chapeau.Il representait un "
        "serpent boa qui digerait un elephant.00000000";
    f1 = 4242;
    f4 = 37373737;
    auto *record_1 = (uint8_t *)record_string_1.data();
    std::memcpy(&record_1[0], &f1, sizeof(f1));
    std::memcpy(
        &record_1[byte_widths_1[1] + byte_widths_1[2] + byte_widths_1[3]], &f4,
        sizeof(f4));

    // Create record_2
    record_string_2 =
        "00000000Twice two makes four is an "
        "excellent thing.Twice two makes five is sometimes a very charming "
        "thing too.00000000";
    f1 = 1776;
    f4 = 1789;
    auto *record_2 = (uint8_t *)record_string_2.data();
    std::memcpy(&record_2[0], &f1, sizeof(f1));
    std::memcpy(
        &record_2[byte_widths_2[1] + byte_widths_2[2] + byte_widths_2[3]], &f4,
        sizeof(f4));

    // Create record_3
    record_string_3 =
        "00000000Nullius in verba.Premature "
        "optimization is the root of all evil.00000000";
    f1 = 481516;
    f4 = 2342;
    auto *record_3 = (uint8_t *)record_string_3.data();
    std::memcpy(&record_3[0], &f1, sizeof(f1));
    std::memcpy(
        &record_3[byte_widths_3[1] + byte_widths_3[2] + byte_widths_3[3]], &f4,
        sizeof(f4));

    // Create record_4
    record_string_4 =
        "00000000When you add verbiage to a page,"
        "you can assume that customers will read 18% of it.00000000";
    f1 = 12020;
    f4 = 7;
    auto *record_4 = (uint8_t *)record_string_4.data();
    std::memcpy(&record_4[0], &f1, sizeof(f1));
    std::memcpy(
        &record_4[byte_widths_4[1] + byte_widths_4[2] + byte_widths_4[3]], &f4,
        sizeof(f4));

    //*****************************

    arrow::Int64Builder col1_builder;
    arrow::StringBuilder col2_builder;
    arrow::StringBuilder col3_builder;
    arrow::Int64Builder col4_builder;

    arrow::Status status;

    status = col1_builder.AppendValues({1, 2, 3});
    status = col2_builder.AppendValues({"nicholas", "edward", "corrado"});
    status = col3_builder.AppendValues({"a", "b", "c"});
    status = col4_builder.AppendValues({11, 22, 33});

    std::shared_ptr<arrow::Int64Array> col1;
    std::shared_ptr<arrow::StringArray> col2;
    std::shared_ptr<arrow::StringArray> col3;
    std::shared_ptr<arrow::Int64Array> col4;

    status = col1_builder.Finish(&col1);
    status = col2_builder.Finish(&col2);
    status = col3_builder.Finish(&col3);
    status = col4_builder.Finish(&col4);

    column_data.push_back(col1->data());
    column_data.push_back(col2->data());
    column_data.push_back(col3->data());
    column_data.push_back(col4->data());
  }

  std::shared_ptr<arrow::Schema> schema;

  std::string record_string_1;
  std::string record_string_2;
  std::string record_string_3;
  std::string record_string_4;

  int32_t byte_widths_1[4] = {8, 42, 56, 8};
  int32_t byte_widths_2[4] = {8, 43, 60, 8};
  int32_t byte_widths_3[4] = {8, 17, 47, 8};
  int32_t byte_widths_4[4] = {8, 32, 50, 8};

  std::shared_ptr<arrow::BooleanArray> valid;
  std::shared_ptr<arrow::Int64Array> column1;
  std::shared_ptr<arrow::StringArray> column2;
  std::shared_ptr<arrow::StringArray> column3;
  std::shared_ptr<arrow::Int64Array> column4;

  std::vector<std::shared_ptr<arrow::ArrayData>> column_data;
};

TEST_F(HustleBlockTest, EmptyBlock) {
  Block block(0, schema, BLOCK_SIZE);

  EXPECT_EQ(block.get_id(), 0);
  EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE);
  EXPECT_EQ(block.get_num_rows(), 0);
  EXPECT_EQ(block.get_records()->num_rows(), 0);
}

TEST_F(HustleBlockTest, OneInsertBlock) {
  Block block(0, schema, BLOCK_SIZE);
  bool result =
      block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);

  valid =
      std::static_pointer_cast<arrow::BooleanArray>(block.get_valid_column());
  column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(0));
  column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(1));
  column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
  column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(3));

  int row = 0;
  EXPECT_EQ(result, true);
  EXPECT_EQ(block.get_num_rows(), 1);
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 4242);
  EXPECT_EQ(column2->GetString(row),
            "Mon dessin ne representait pas un chapeau.");
  EXPECT_EQ(column3->GetString(row),
            "Il representait un serpent boa qui digerait un elephant.");
  EXPECT_EQ(column4->Value(row), 37373737);
}

TEST_F(HustleBlockTest, ManyInsertBlock) {
  Block block(0, schema, BLOCK_SIZE);
  block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);
  block.insert_record((uint8_t *)record_string_2.data(), byte_widths_2);
  block.insert_record((uint8_t *)record_string_3.data(), byte_widths_3);
  block.insert_record((uint8_t *)record_string_4.data(), byte_widths_4);

  valid =
      std::static_pointer_cast<arrow::BooleanArray>(block.get_valid_column());
  column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(0));
  column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(1));
  column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
  column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(3));

  int num_bytes = record_string_1.length() + record_string_2.length() +
                  record_string_3.length() + record_string_4.length();

  EXPECT_EQ(block.get_num_rows(), 4);
  EXPECT_EQ((block.get_bytes_left()), BLOCK_SIZE - num_bytes);

  int row = 0;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 4242);
  EXPECT_EQ(column2->GetString(row),
            "Mon dessin ne representait pas un chapeau.");
  EXPECT_EQ(column3->GetString(row),
            "Il representait un serpent boa qui digerait un elephant.");
  EXPECT_EQ(column4->Value(row), 37373737);

  row = 1;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 1776);
  EXPECT_EQ(column2->GetString(row),
            "Twice two makes four is an excellent thing.");
  EXPECT_EQ(column3->GetString(row),
            "Twice two makes five is sometimes a very charming thing too.");
  EXPECT_EQ(column4->Value(row), 1789);

  row = 2;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 481516);
  EXPECT_EQ(column2->GetString(row), "Nullius in verba.");
  EXPECT_EQ(column3->GetString(row),
            "Premature optimization is the root of all evil.");
  EXPECT_EQ(column4->Value(row), 2342);

  row = 3;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 12020);
  EXPECT_EQ(column2->GetString(row), "When you add verbiage to a page,");
  EXPECT_EQ(column3->GetString(row),
            "you can assume that customers will read 18% of it.");
  EXPECT_EQ(column4->Value(row), 7);
}

TEST_F(HustleBlockTest, FullBlock) {
  Block block(0, schema, BLOCK_SIZE);

  // With 1 KB block size, we can store 8 copies of the first record
  for (int i = 0; i < 8; i++) {
    block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);
  }

  // This block cannot hold a 9th copy of the first record
  bool result =
      block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);

  EXPECT_EQ(result, false);
  EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE - 912);
}

TEST_F(HustleBlockTest, ArrayInsert) {
  auto test_schema = arrow::schema(
      {arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64()),
       arrow::field("C", arrow::int64()), arrow::field("D", arrow::int64())});

  auto record_batch = arrow::RecordBatch::Make(test_schema, 5, column_data);

  Block block(0, schema, BLOCK_SIZE);
  block.insert_records(column_data);

  int row;
  valid =
      std::static_pointer_cast<arrow::BooleanArray>(block.get_valid_column());
  column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(0));
  column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(1));
  column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
  column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(3));

  EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE - 72);

  row = 0;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 1);
  EXPECT_EQ(column2->GetString(row), "nicholas");
  EXPECT_EQ(column3->GetString(row), "a");
  EXPECT_EQ(column4->Value(row), 11);

  row = 1;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 2);
  EXPECT_EQ(column2->GetString(row), "edward");
  EXPECT_EQ(column3->GetString(row), "b");
  EXPECT_EQ(column4->Value(row), 22);

  row = 2;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 3);
  EXPECT_EQ(column2->GetString(row), "corrado");
  EXPECT_EQ(column3->GetString(row), "c");
  EXPECT_EQ(column4->Value(row), 33);
}

TEST_F(HustleBlockTest, ArrayAndSingleInsert) {
  auto test_schema = arrow::schema(
      {arrow::field("A", arrow::int64()), arrow::field("B", arrow::int64()),
       arrow::field("C", arrow::int64()), arrow::field("D", arrow::int64())});

  auto record_batch = arrow::RecordBatch::Make(test_schema, 5, column_data);
  auto in_offsets_data = column_data[2]->GetMutableValues<int32_t>(1, 0);
  int off = in_offsets_data[0];

  Block block(0, schema, BLOCK_SIZE);
  block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);
  block.insert_records(column_data);
  block.insert_record((uint8_t *)record_string_1.data(), byte_widths_1);
  block.insert_records(column_data);

  int row;
  valid =
      std::static_pointer_cast<arrow::BooleanArray>(block.get_valid_column());
  column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(0));
  column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(1));
  column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
  column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(3));

  EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE - 372);

  row = 0;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 4242);
  EXPECT_EQ(column2->GetString(row),
            "Mon dessin ne representait pas un chapeau.");
  EXPECT_EQ(column3->GetString(row),
            "Il representait un serpent boa qui digerait un elephant.");
  EXPECT_EQ(column4->Value(row), 37373737);

  row = 1;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 1);
  EXPECT_EQ(column2->GetString(row), "nicholas");
  EXPECT_EQ(column3->GetString(row), "a");
  EXPECT_EQ(column4->Value(row), 11);

  row = 2;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 2);
  EXPECT_EQ(column2->GetString(row), "edward");
  EXPECT_EQ(column3->GetString(row), "b");
  EXPECT_EQ(column4->Value(row), 22);

  row = 3;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 3);
  EXPECT_EQ(column2->GetString(row), "corrado");
  EXPECT_EQ(column3->GetString(row), "c");
  EXPECT_EQ(column4->Value(row), 33);

  row = 4;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 4242);
  EXPECT_EQ(column2->GetString(row),
            "Mon dessin ne representait pas un chapeau.");
  EXPECT_EQ(column3->GetString(row),
            "Il representait un serpent boa qui digerait un elephant.");
  EXPECT_EQ(column4->Value(row), 37373737);
}

TEST_F(HustleBlockTest, BlockFromRecordBatch) {
  auto record_batch_1 = arrow::RecordBatch::Make(schema, 3, column_data);

  Block block(0, record_batch_1, BLOCK_SIZE);

  int row;
  valid =
      std::static_pointer_cast<arrow::BooleanArray>(block.get_valid_column());
  column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(0));
  column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(1));
  column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
  column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(3));

  EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE - 72);

  row = 0;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 1);
  EXPECT_EQ(column2->GetString(row), "nicholas");
  EXPECT_EQ(column3->GetString(row), "a");
  EXPECT_EQ(column4->Value(row), 11);

  row = 1;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 2);
  EXPECT_EQ(column2->GetString(row), "edward");
  EXPECT_EQ(column3->GetString(row), "b");
  EXPECT_EQ(column4->Value(row), 22);

  row = 2;
  EXPECT_EQ(valid->Value(row), true);
  EXPECT_EQ(column1->Value(row), 3);
  EXPECT_EQ(column2->GetString(row), "corrado");
  EXPECT_EQ(column3->GetString(row), "c");
  EXPECT_EQ(column4->Value(row), 33);
}
