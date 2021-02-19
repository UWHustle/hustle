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

#include "storage/table.h"

#include <arrow/io/api.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "api/hustle_db.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/util.h"

#define BLOCK_SIZE \
  1024  // Force the block size to 1 KB for the sake of this test

using namespace testing;
using namespace hustle::storage;

class HustleTableTest : public testing::Test {
 protected:
  arrow::Status status;
  std::shared_ptr<arrow::Schema> schema;
  std::string record_string;
  int32_t byte_widths[4] = {8, 42, 56, 8};

  std::vector<std::shared_ptr<arrow::ArrayData>> column_data;

  void SetUp() override {
    std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
    std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
    std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
    std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
    schema = arrow::schema({field1, field2, field3, field4});

    record_string =
        "00000000Mon dessin ne representait pas un chapeau.Il representait un "
        "serpent boa qui digerait un elephant.00000000";
    int64_t f1 = 4242;
    int64_t f4 = 37373737;
    auto *record_1 = (uint8_t *)record_string.data();
    std::memcpy(&record_1[0], &f1, sizeof(f1));
    std::memcpy(&record_1[byte_widths[1] + byte_widths[2] + byte_widths[3]],
                &f4, sizeof(f4));

    std::ofstream csv_file;
    csv_file.open("table_test.csv");
    for (int i = 0; i < 9; i++) {
      csv_file
          << "4242|Mon dessin ne representait pas un chapeau.|Il representait "
             "un serpent boa qui digerait un elephant.|37373737\n";
    }
    csv_file.close();

    //*****************************

    arrow::Int64Builder col1_builder;
    arrow::StringBuilder col2_builder;
    arrow::StringBuilder col3_builder;
    arrow::Int64Builder col4_builder;

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
};

TEST_F(HustleTableTest, EmptyTable) {
  DBTable table("table", schema, BLOCK_SIZE);
  EXPECT_EQ(table.get_num_blocks(), 0);
}

TEST_F(HustleTableTest, OneBlockTable) {
  DBTable table("table", schema, BLOCK_SIZE);
  table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  EXPECT_EQ(table.get_num_blocks(), 1);
}

TEST_F(HustleTableTest, OneBlockArray) {
  DBTable table("table", schema, BLOCK_SIZE);
  table.InsertRecords(column_data);
  EXPECT_EQ(table.get_num_blocks(), 1);
}

TEST_F(HustleTableTest, TwoBlockArray) {
  DBTable table("table", schema, BLOCK_SIZE);
  // Inserting three records 14 times. This fits into one block.
  for (int i = 0; i < 14; i++) {
    table.InsertRecords(column_data);
  }
  // Bulk inserting again should allocate a new block
  table.InsertRecords(column_data);
  EXPECT_EQ(table.get_num_blocks(), 2);
}

TEST_F(HustleTableTest, TwoBlockTable) {
  DBTable table("table", schema, BLOCK_SIZE);
  // With 1 KB block size, we can store 8 copies of the first record in one
  // block.
  for (int i = 0; i < 8; i++) {
    table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  }
  // The first block cannot hold a 9th copy of the first record, so we must
  // create a new block.
  table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  EXPECT_EQ(table.get_num_blocks(), 2);
}

TEST_F(HustleTableTest, TableIO) {
  DBTable table("table", schema, BLOCK_SIZE);
  table.InsertRecords(column_data);
  table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  table.InsertRecords(column_data);
  for (int i = 0; i < 8; i++) {
    table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  }
  // The first block cannot hold a 9th copy of the first record, so we must
  // create a new block.
  table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  write_to_file("table.hsl", table);
  auto table_from_file = read_from_file("table.hsl");
  EXPECT_TRUE(table_from_file->get_block(0)->get_valid_column()->Equals(
      *table.get_block(0)->get_valid_column()));
  EXPECT_TRUE(table_from_file->get_block(0)->get_records()->Equals(
      *table.get_block(0)->get_records()));
  EXPECT_TRUE(table_from_file->get_block(1)->get_records()->Equals(
      *table.get_block(1)->get_records()));
}

TEST_F(HustleTableTest, ReadTableFromCSV) {
  DBTable table("table", schema, BLOCK_SIZE);
  // With 1 KB block size, we can store 8 copies of the first record in one
  // block.
  for (int i = 0; i < 8; i++) {
    table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  }
  // The first block cannot hold a 9th copy of the first record, so we must
  // create a new block.
  table.InsertRecord((uint8_t *)record_string.data(), byte_widths);
  auto table_from_csv =
      read_from_csv_file("table_test.csv", schema, BLOCK_SIZE);
  EXPECT_TRUE(table_from_csv->get_block(0)->get_records()->Equals(
      *table.get_block(0)->get_records()));
  EXPECT_TRUE(table_from_csv->get_block(1)->get_records()->Equals(
      *table.get_block(1)->get_records()));
}

TEST_F(HustleTableTest, Insert) {
  // Create table customer
  hustle::catalog::TableSchema customer("customer_table_test");
  hustle::catalog::ColumnSchema c_suppkey(
      "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c_name(
      "c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_address(
      "c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_city(
      "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  hustle::catalog::ColumnSchema c_nation(
      "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_region(
      "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
  hustle::catalog::ColumnSchema c_phone(
      "c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_mktsegment(
      "c_mktsegment", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  customer.addColumn(c_suppkey);
  customer.addColumn(c_name);
  customer.addColumn(c_address);
  customer.addColumn(c_city);
  customer.addColumn(c_nation);
  customer.addColumn(c_region);
  customer.addColumn(c_phone);
  customer.addColumn(c_mktsegment);
  customer.setPrimaryKey({});
  std::shared_ptr<arrow::Schema> c_schema = customer.getArrowSchema();

  std::string query =
      "BEGIN TRANSACTION; "
      "INSERT INTO customer_table_test VALUES (800224, 'James', "
      " 'good',"
      "'Houston', 'Great',"
      "         'best', 'fit', 'done');"
      "INSERT INTO customer_table_test VALUES (800225, 'James1', "
      " 'good1',"
      "'Houston1', 'Great1',"
      "         'best', 'fit', 'done');"
      "COMMIT;";
  DBTable::TablePtr c =
      std::make_shared<DBTable>("customer_table_test", c_schema, BLOCK_SIZE);
  hustle::HustleDB hustleDB("db_directory_insert");
  hustleDB.createTable(customer, c);
  hustleDB.executeQuery(query);
  EXPECT_EQ(c->get_num_rows(), 2);
  EXPECT_EQ(c->get_num_cols(), 8);
  std::filesystem::remove_all("db_directory_insert");
}

TEST_F(HustleTableTest, Update) {
  // Create table customer
  hustle::catalog::TableSchema customer_table("customer_table");
  hustle::catalog::ColumnSchema c_suppkey(
      "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c_name(
      "c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_address(
      "c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_city(
      "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  hustle::catalog::ColumnSchema c_nation(
      "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_region(
      "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
  hustle::catalog::ColumnSchema c_phone(
      "c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_mktsegment(
      "c_mktsegment", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  customer_table.addColumn(c_suppkey);
  customer_table.addColumn(c_name);
  customer_table.addColumn(c_address);
  customer_table.addColumn(c_city);
  customer_table.addColumn(c_nation);
  customer_table.addColumn(c_region);
  customer_table.addColumn(c_phone);
  customer_table.addColumn(c_mktsegment);
  customer_table.setPrimaryKey({});
  std::shared_ptr<arrow::Schema> customer_schema =
      customer_table.getArrowSchema();
  std::string query =
      "BEGIN TRANSACTION; "
      "INSERT INTO customer_table VALUES (800224, 'James', "
      " 'good',"
      "'Houston', 'Great',"
      "         'best', 'fit', 12);"
      "COMMIT;";
  DBTable::TablePtr customer_table_ptr =
      std::make_shared<DBTable>("customer_table", customer_schema, BLOCK_SIZE);
  hustle::HustleDB hustleDB("db_directory_update");
  hustleDB.createTable(customer_table, customer_table_ptr);
  hustleDB.executeQuery(query);

  query =
      "BEGIN TRANSACTION;"
      "UPDATE customer_table set c_region = 'fine' where c_custkey=800224;"
      "COMMIT;";

  hustleDB.executeQuery(query);
  auto col = std::static_pointer_cast<arrow::StringArray>(
      customer_table_ptr->get_column(5)->chunk(0));
  EXPECT_EQ(col->GetString(0), "fine");
  EXPECT_EQ(customer_table_ptr->get_num_rows(), 1);
  EXPECT_EQ(customer_table_ptr->get_num_blocks(), 1);
  EXPECT_EQ(customer_table_ptr->get_num_cols(), 8);
  auto int_col = std::static_pointer_cast<arrow::Int64Array>(
      customer_table_ptr->get_column(7)->chunk(0));
  EXPECT_EQ(int_col->Value(0), 12);
  query =
      "BEGIN TRANSACTION;"
      "UPDATE customer_table set c_mktsegment = 1123 where c_custkey=800224;"
      "COMMIT;";
  hustleDB.executeQuery(query);
  EXPECT_EQ(int_col->Value(0), 1123);
  EXPECT_EQ(customer_table_ptr->get_num_rows(), 1);
  EXPECT_EQ(customer_table_ptr->get_num_blocks(), 1);
  EXPECT_EQ(customer_table_ptr->get_num_cols(), 8);
  std::filesystem::remove_all("db_directory_update");
}

TEST_F(HustleTableTest, Delete) {
  // Create table customer
  hustle::catalog::TableSchema customer_table("customer_table_d");
  hustle::catalog::ColumnSchema c_suppkey(
      "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c_name(
      "c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_address(
      "c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_city(
      "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  hustle::catalog::ColumnSchema c_nation(
      "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_region(
      "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
  hustle::catalog::ColumnSchema c_phone(
      "c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_mktsegment(
      "c_mktsegment", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  customer_table.addColumn(c_suppkey);
  customer_table.addColumn(c_name);
  customer_table.addColumn(c_address);
  customer_table.addColumn(c_city);
  customer_table.addColumn(c_nation);
  customer_table.addColumn(c_region);
  customer_table.addColumn(c_phone);
  customer_table.addColumn(c_mktsegment);
  customer_table.setPrimaryKey({});
  std::shared_ptr<arrow::Schema> customer_schema =
      customer_table.getArrowSchema();
  std::string query =
      "BEGIN TRANSACTION; "
      "INSERT INTO customer_table_d VALUES (800224, 'James', "
      " 'good',"
      "'Houston', 'Great',"
      "         'best', 'fit', 'done');"
      "COMMIT;";
  DBTable::TablePtr customer_table_ptr = std::make_shared<DBTable>(
      "customer_table_d", customer_schema, BLOCK_SIZE);
  hustle::HustleDB hustleDB("db_directory_delete");
  hustleDB.createTable(customer_table, customer_table_ptr);
  hustleDB.executeQuery(query);
  query =
      "BEGIN TRANSACTION;"
      "DELETE FROM customer_table_d where c_custkey=800224;"
      "COMMIT;";
  hustleDB.executeQuery(query);
  auto col = std::static_pointer_cast<arrow::StringArray>(
      customer_table_ptr->get_column(5)->chunk(0));
  EXPECT_EQ(customer_table_ptr->get_num_rows(), 0);
  EXPECT_EQ(customer_table_ptr->get_num_blocks(), 1);
  EXPECT_EQ(customer_table_ptr->get_num_cols(), 8);
  std::filesystem::remove_all("db_directory_delete");
}

TEST_F(HustleTableTest, Load) {
  // Create table customer
  hustle::catalog::TableSchema customer("customer_table_test");
  hustle::catalog::ColumnSchema c_suppkey(
      "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c_name(
      "c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_address(
      "c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
  hustle::catalog::ColumnSchema c_city(
      "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  hustle::catalog::ColumnSchema c_nation(
      "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_region(
      "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
  hustle::catalog::ColumnSchema c_phone(
      "c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema c_mktsegment(
      "c_mktsegment", {hustle::catalog::HustleType::CHAR, 10}, true, false);
  customer.addColumn(c_suppkey);
  customer.addColumn(c_name);
  customer.addColumn(c_address);
  customer.addColumn(c_city);
  customer.addColumn(c_nation);
  customer.addColumn(c_region);
  customer.addColumn(c_phone);
  customer.addColumn(c_mktsegment);
  customer.setPrimaryKey({});
  std::shared_ptr<arrow::Schema> c_schema = customer.getArrowSchema();

  std::string query =
      "BEGIN TRANSACTION; "
      "INSERT INTO customer_table_test VALUES (800224, 'James', "
      " 'good',"
      "'Houston', 'Great',"
      "         'best', 'fit', 'done');"
      "INSERT INTO customer_table_test VALUES (800225, 'James1', "
      " 'good1',"
      "'Houston1', 'Great1',"
      "         'best', 'fit', 'done');"
      "COMMIT;";
  DBTable::TablePtr c =
      std::make_shared<DBTable>("customer_table_test", c_schema, BLOCK_SIZE);
  hustle::HustleDB hustleDB("db_directory_load");
  hustleDB.createTable(customer, c);
  hustleDB.executeQuery(query);
  EXPECT_EQ(c->get_num_rows(), 2);
  EXPECT_EQ(c->get_num_cols(), 8);

  hustleDB.dropMemTable("customer_table_test");
  c = std::make_shared<DBTable>("customer_table_test", c_schema, BLOCK_SIZE);
  hustleDB.createTable(customer, c);
  hustleDB.loadTables();
  EXPECT_EQ(c->get_num_rows(), 2);

  query =
      "BEGIN TRANSACTION;"
      "DELETE FROM customer_table_test where c_custkey=800224;"
      "COMMIT;";
  hustleDB.executeQuery(query);
  EXPECT_EQ(c->get_num_rows(), 1);

  query =
      "BEGIN TRANSACTION;"
      "UPDATE customer_table_test set c_mktsegment = 1123 where "
      "c_custkey=800225;"
      "COMMIT;";
  hustleDB.executeQuery(query);

  auto int_col =
      std::static_pointer_cast<arrow::StringArray>(c->get_column(1)->chunk(0));
  EXPECT_EQ(int_col->GetString(0), "James1");
  EXPECT_EQ(c->get_num_rows(), 1);
  std::filesystem::remove_all("db_directory_load");
}
