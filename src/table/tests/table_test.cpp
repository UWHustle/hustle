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

// TODO(nicholas): Talk to Yannis about structuring tests. Also, when you split
// this into multiple tests, you should define a test fixture so you can reuse
// the same data configurations for different tests without reinitializing
// everything.
TEST(HustleTable, EmptyTable) {

    int row;
    int num_bytes = 0;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
    std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
    std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
    std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
    std::shared_ptr<arrow::Schema> schema = arrow::schema({field1, field2, field3, field4});

    std::string record_string;
    uint8_t *record_bytes;
    int32_t byte_widths[4] = {8, 0, 0, 8};
    uint64_t f1, f4;

    Table table("table", schema, BLOCK_SIZE);
    EXPECT_EQ(table.get_num_blocks(), 0);

    // Create tuple
    record_string = "00000000Mon dessin ne representait pas un chapeau.Il representait un serpent boa qui digerait un elephant.00000000";
    byte_widths[1] = 42;
    byte_widths[2] = 56;
    f1 = 4242;
    f4 = 37373737;
    record_bytes = (uint8_t *) record_string.data();
    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[byte_widths[1] + byte_widths[2] + byte_widths[3]], &f4, sizeof(f4));

    table.insert_record(record_bytes, byte_widths);

    // Fetch each column. We must refetch columns after each insertion in case
    // memory is reallocated upon insertion.
    valid = std::static_pointer_cast<arrow::BooleanArray>(
            table.get_block(0)->get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(4));

    num_bytes += record_string.length();
    row = 0;
    EXPECT_EQ(table.get_num_blocks(), 1);
    EXPECT_EQ(table.get_block(0)->get_num_rows(), 1);
    EXPECT_EQ(valid->Value(row), true);
    EXPECT_EQ(column1->Value(row), f1);
    EXPECT_EQ(column2->GetString(row), "Mon dessin ne representait pas un chapeau.");
    EXPECT_EQ(column3->GetString(row), "Il representait un serpent boa qui digerait un elephant.");
    EXPECT_EQ(column4->Value(row), f4);

    record_string = "00000000Twice two makes four is an excellent thing.Twice two makes five is sometimes a very charming thing too.00000000";
    byte_widths[1] = 43;
    byte_widths[2] = 60;
    f1 = 1776;
    f4 = 1789;
    record_bytes = (uint8_t *) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[byte_widths[1] + byte_widths[2] + byte_widths[3]],
            &f4, sizeof(f4));
    table.insert_record(record_bytes, byte_widths);

    valid = std::static_pointer_cast<arrow::BooleanArray>(
            table.get_block(0)->get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(4));

    num_bytes += record_string.length();
    row = 1;
    EXPECT_EQ(table.get_num_blocks(), 1);
    EXPECT_EQ(table.get_block(0)->get_num_rows(), 2);
    EXPECT_EQ(valid->Value(row), true);
    EXPECT_EQ(column1->Value(row), f1);
    EXPECT_EQ(column2->GetString(row), "Twice two makes four is an excellent thing.");
    EXPECT_EQ(column3->GetString(row), "Twice two makes five is sometimes a very charming thing too.");
    EXPECT_EQ(column4->Value(row), f4);

    record_string = "00000000Nullius in verba.Premature optimization is the root of all evil.00000000";
    byte_widths[1] = 17;
    byte_widths[2] = 47;
    f1 = 481516;
    f4 = 2342;
    record_bytes = (uint8_t *) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[byte_widths[1] + byte_widths[2] + byte_widths[3]], &f4, sizeof(f4));
    table.insert_record(record_bytes, byte_widths);

    valid = std::static_pointer_cast<arrow::BooleanArray>(
            table.get_block(0)->get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(
            table.get_block(0)->get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(
            table.get_block(0)->get_column(4));

    num_bytes += record_string.length();
    row = 2;
    EXPECT_EQ(table.get_num_blocks(), 1);
    EXPECT_EQ(table.get_block(0)->get_num_rows(), 3);
    EXPECT_EQ(valid->Value(row), true);
    EXPECT_EQ(column1->Value(row), f1);
    EXPECT_EQ(column2->GetString(row), "Nullius in verba.");
    EXPECT_EQ(column3->GetString(row), "Premature optimization is the root of all evil.");
    EXPECT_EQ(column4->Value(row), f4);

    write_to_file("./output.arrow", table);
    Table table_from_file = read_from_file("./output.arrow");

    // Check that the record batch read from the file is the same as the one
    // written to the file.
    EXPECT_TRUE(table.get_block(0)->get_records()->Equals(
            *table_from_file.get_block(0)->get_records()));

    record_string = "00000000When you add verbiage to a page,you can assume that customers will read 18% of it.00000000";
    byte_widths[1] = 32;
    byte_widths[2] = 50;
    f1 = 12020;
    f4 = 7;
    record_bytes = (uint8_t *) record_string.data();
    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[byte_widths[1] + byte_widths[2] + byte_widths[3]],
            &f4, sizeof(f4));

    // N = the number of times we can re-insert the third record without
    // needing to create a new block.
    int N = table_from_file.get_block(0)->get_bytes_left() / 98;

    // Insert enough records such that we must create a new block containing one
    // record.
    for (int i = 0; i < N + 1; i++) {
        table_from_file.insert_record(record_bytes, byte_widths);
    }

    // At this point, we should have two blocks. The first block should be full,
    // and the second block should have only one tuple.

    EXPECT_EQ(table_from_file.get_num_blocks(), 2);
    EXPECT_EQ(table_from_file.get_block(0)->get_bytes_left(), BLOCK_SIZE - (313 + 98 * 7));
    EXPECT_EQ(table_from_file.get_block(1)->get_bytes_left(), BLOCK_SIZE - 98);

    valid = std::static_pointer_cast<arrow::BooleanArray>(
            table_from_file.get_block(0)->get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(
            table_from_file.get_block(0)->get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(
            table_from_file.get_block(0)->get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(
            table_from_file.get_block(0)->get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(
            table_from_file.get_block(0)->get_column(4));

    // Check the contents of tuples in the first block
    for (int i = 0; i < N + 3; i++) {

        EXPECT_EQ(valid->Value(i), true);

        if (i == 0) {
            EXPECT_EQ(column1->Value(i), 4242);
            EXPECT_EQ(column2->GetString(i), "Mon dessin ne representait pas un chapeau.");
            EXPECT_EQ(column3->GetString(i), "Il representait un serpent boa qui digerait un elephant.");
            EXPECT_EQ(column4->Value(i), 37373737);
        } else if (i == 1) {
            EXPECT_EQ(column1->Value(i), 1776);
            EXPECT_EQ(column2->GetString(i), "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(i), "Twice two makes five is sometimes a very charming thing too.");
            EXPECT_EQ(column4->Value(i), 1789);
        } else if (i == 2) {
            EXPECT_EQ(column1->Value(i), 481516);
            EXPECT_EQ(column2->GetString(i), "Nullius in verba.");
            EXPECT_EQ(column3->GetString(i), "Premature optimization is the root of all evil.");
            EXPECT_EQ(column4->Value(i), 2342);
        } else {
            EXPECT_EQ(column1->Value(i), 12020);
            EXPECT_EQ(column2->GetString(i), "When you add verbiage to a page,");
            EXPECT_EQ(column3->GetString(i), "you can assume that customers will read 18% of it.");
            EXPECT_EQ(column4->Value(i), 7);
        }
    }

    valid = std::static_pointer_cast<arrow::BooleanArray>(
            table_from_file.get_block(1)->get_column(0));
    column1 = std::static_pointer_cast<arrow::Int64Array>(
            table_from_file.get_block(1)->get_column(1));
    column2 = std::static_pointer_cast<arrow::StringArray>(
            table_from_file.get_block(1)->get_column(2));
    column3 = std::static_pointer_cast<arrow::StringArray>(
            table_from_file.get_block(1)->get_column(3));
    column4 = std::static_pointer_cast<arrow::Int64Array>(
            table_from_file.get_block(1)->get_column(4));

    // Check the contents of the tuple in the second block.
    EXPECT_EQ(column1->Value(0), 12020);
    EXPECT_EQ(column2->GetString(0), "When you add verbiage to a page,");
    EXPECT_EQ(column3->GetString(0), "you can assume that customers will read 18% of it.");
    EXPECT_EQ(column4->Value(0), 7);
}