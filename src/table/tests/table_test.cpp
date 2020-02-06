#include <iostream>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <arrow/io/api.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "table/table.h"
#include "table/util.h"

#define BLOCK_SIZE 1024 // Force the block size to 1 KB for the sake of this test

using namespace testing;


class HustleTableTest: public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;
    std::string record_string;
    int32_t byte_widths[4] = {8, 42, 56, 8};

    void SetUp() override {

        std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
        std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
        std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
        schema = arrow::schema(
                {field1, field2, field3, field4});

        record_string = "00000000Mon dessin ne representait pas un chapeau.Il representait un serpent boa qui digerait un elephant.00000000";
        int64_t f1 = 4242;
        int64_t f4 = 37373737;
        auto* record_1 = (uint8_t *) record_string.data();
        std::memcpy(&record_1[0], &f1, sizeof(f1));
        std::memcpy(&record_1[byte_widths[1] + byte_widths[2] +
                              byte_widths[3]], &f4, sizeof(f4));

        std::ofstream csv_file;
        csv_file.open("table_test.csv");
        for (int i=0; i<9; i++) {
            csv_file << "4242|Mon dessin ne representait pas un chapeau.|Il representait un serpent boa qui digerait un elephant.|37373737\n";
        }
        csv_file.close();

    }

};

TEST_F(HustleTableTest, EmptyTable) {

    Table table("table", schema, BLOCK_SIZE);

    EXPECT_EQ(table.get_num_blocks(), 0);
}

TEST_F(HustleTableTest, OneBlockTable) {

    Table table("table", schema, BLOCK_SIZE);

    table.insert_record((uint8_t *) record_string.data(), byte_widths);

    EXPECT_EQ(table.get_num_blocks(), 1);
}

TEST_F(HustleTableTest, TwoBlockTable) {

    Table table("table", schema, BLOCK_SIZE);

    // With 1 KB block size, we can store 8 copies of the first record in one
    // block.
    for (int i=0 ; i<8; i++) {
        table.insert_record((uint8_t*) record_string.data(), byte_widths);
    }

    // The first block cannot hold a 9th copy of the first record, so we must
    // create a new block.
    table.insert_record((uint8_t*) record_string.data(),
            byte_widths);

    EXPECT_EQ(table.get_num_blocks(), 2);
}

TEST_F(HustleTableTest, ReadTableFromFile) {

    Table table("table", schema, BLOCK_SIZE);

    // With 1 KB block size, we can store 8 copies of the first record in one
    // block.
    for (int i=0 ; i<8; i++) {
        table.insert_record((uint8_t*) record_string.data(), byte_widths);
    }

    // The first block cannot hold a 9th copy of the first record, so we must
    // create a new block.
    table.insert_record((uint8_t*) record_string.data(),
                        byte_widths);

    write_to_file("table.hsl", table);
    Table table_from_file = read_from_file("table.hsl");

    EXPECT_TRUE(table_from_file.get_block(0)->get_records()->
            Equals(*table.get_block(0)->get_records()));
    EXPECT_TRUE(table_from_file.get_block(1)->get_records()->
            Equals(*table.get_block(1)->get_records()));

}

TEST_F(HustleTableTest, ReadTableFromCSV) {

    Table table("table", schema, BLOCK_SIZE);

    // With 1 KB block size, we can store 8 copies of the first record in one
    // block.
    for (int i=0 ; i<8; i++) {
        table.insert_record((uint8_t*) record_string.data(), byte_widths);
    }

    // The first block cannot hold a 9th copy of the first record, so we must
    // create a new block.
    table.insert_record((uint8_t*) record_string.data(),
                        byte_widths);

    Table table_from_csv = read_from_csv_file
            ("table_test.csv", schema, BLOCK_SIZE);

    EXPECT_TRUE(table_from_csv.get_block(0)->get_records()->
            Equals(*table.get_block(0)->get_records()));
    EXPECT_TRUE(table_from_csv.get_block(1)->get_records()->
            Equals(*table.get_block(1)->get_records()));
}
