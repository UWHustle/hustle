#include <iostream>
#include <iostream>
#include <filesystem>
#include <arrow/io/api.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "table/table.h"
#include "table/util.h"

#define BLOCK_SIZE 1024 // Force the block size to 1 KB for the sake of this test

using namespace testing;

class HustleBlockTest: public testing::Test {
protected:

    void SetUp() override {

        std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
        std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
        std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
        schema = arrow::schema(
                {field1, field2, field3, field4});

        uint64_t f1, f4;

        // Create record_1
        record_string_1 = "00000000Mon dessin ne representait pas un chapeau.Il representait un serpent boa qui digerait un elephant.00000000";
        f1 = 4242;
        f4 = 37373737;
        auto* record_1 = (uint8_t *) record_string_1.data();
        std::memcpy(&record_1[0], &f1, sizeof(f1));
        std::memcpy(&record_1[byte_widths_1[1] + byte_widths_1[2] +
        byte_widths_1[3]], &f4, sizeof(f4));

        // Create record_2
        record_string_2 = "00000000Twice two makes four is an "
                                  "excellent thing.Twice two makes five is sometimes a very charming thing too.00000000";
        f1 = 1776;
        f4 = 1789;
        auto* record_2 = (uint8_t *) record_string_2.data();
        std::memcpy(&record_2[0], &f1, sizeof(f1));
        std::memcpy(&record_2[byte_widths_2[1] + byte_widths_2[2] +
        byte_widths_2[3]], &f4, sizeof(f4));

        // Create record_3
        record_string_3 = "00000000Nullius in verba.Premature "
                               "optimization is the root of all evil.00000000";
        f1 = 481516;
        f4 = 2342;
        auto* record_3 = (uint8_t *) record_string_3.data();
        std::memcpy(&record_3[0], &f1, sizeof(f1));
        std::memcpy(&record_3[byte_widths_3[1] + byte_widths_3[2] +
        byte_widths_3[3]],&f4, sizeof(f4));

        // Create record_4
        record_string_4 = "00000000When you add verbiage to a page,"
                                   "you can assume that customers will read 18% of it.00000000";
        f1 = 12020;
        f4 = 7;
        auto* record_4 = (uint8_t *) record_string_4.data();
        std::memcpy(&record_4[0], &f1, sizeof(f1));
        std::memcpy(&record_4[byte_widths_4[1] + byte_widths_4[2] +
        byte_widths_4[3]], &f4, sizeof(f4));

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
    bool result = block.insert_record((uint8_t*) record_string_1.data(),
            byte_widths_1);

    valid = std::static_pointer_cast<arrow::BooleanArray>(block.get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(4));

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
    block.insert_record((uint8_t*) record_string_1.data(), byte_widths_1);
    block.insert_record((uint8_t*) record_string_2.data(), byte_widths_2);
    block.insert_record((uint8_t*) record_string_3.data(), byte_widths_3);
    block.insert_record((uint8_t*) record_string_4.data(), byte_widths_4);

    valid = std::static_pointer_cast<arrow::BooleanArray>(block.get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(block.get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(block.get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(block.get_column(4));

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
    EXPECT_EQ(column2->GetString(row),
              "When you add verbiage to a page,");
    EXPECT_EQ(column3->GetString(row),
              "you can assume that customers will read 18% of it.");
    EXPECT_EQ(column4->Value(row), 7);

    block.print();
}

TEST_F(HustleBlockTest, FullBlock) {

    Block block(0, schema, BLOCK_SIZE);

    // With 1 KB block size, we can store 8 copies of the first record
    for (int i=0 ; i<8; i++) {
        block.insert_record((uint8_t*) record_string_1.data(), byte_widths_1);
    }

    // This block cannot hold a 9th copy of the first record
    bool result = block.insert_record((uint8_t*) record_string_1.data(), byte_widths_1);

    EXPECT_EQ(result, false);
    EXPECT_EQ(block.get_bytes_left(), BLOCK_SIZE - 912);
}