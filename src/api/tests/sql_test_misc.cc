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

#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "operators/select/predicate.h"
#include "resolver/cresolver.h"
#include "resolver/select_resolver.h"
#include "sqlite3/sqlite3.h"
#include "storage/utils/util.h"
#include "utils/sqlite_utils_parse.h"
#include "utils/string_utils.h"

using namespace testing;
using namespace hustle::resolver;

#define BLOCK_SIZE \
  1024  // Force the block size to small size for the sake of this test

#define ROOT_PATH "../../../test/data/"
#define LINE_ORDER_PATH ROOT_PATH "lineorder.tbl"
#define SUPPLIER_PATH ROOT_PATH "supplier.tbl"
#define CUSTOMER_PATH ROOT_PATH "customer.tbl"
#define PART_PATH ROOT_PATH "part.tbl"
#define DATE_PATH ROOT_PATH "date.tbl"

class SQLTest : public Test {
 public:
  static hustle::catalog::TableSchema part, supplier, customer, ddate,
      lineorder;
  static DBTable::TablePtr lo, d, p, c, s;
  static std::shared_ptr<hustle::HustleDB> hustle_db;

  char** getfields(char* line, int num) {
    char** fields = (char**)malloc(num * sizeof(char*));
    char* field;
    int index = 0;
    for (field = strtok(line, "|"); field && *field;
         field = strtok(NULL, "|\n")) {
      fields[index++] = field;
    }
    return fields;
  }

  void SetUp() override {
    std::filesystem::remove_all("db_directory_sql");
    EXPECT_FALSE(std::filesystem::exists("db_directory_sql"));

    SQLTest::hustle_db = std::make_shared<hustle::HustleDB>("db_directory_sql");

    // Create table part
    // hustle::catalog::TableSchema part("part");
    hustle::catalog::ColumnSchema p_partkey(
        "p_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema p_mfgr(
        "p_mfgr", {hustle::catalog::HustleType::CHAR, 6}, true, false);
    hustle::catalog::ColumnSchema p_category(
        "p_category", {hustle::catalog::HustleType::CHAR, 7}, true, false);
    hustle::catalog::ColumnSchema p_brand1(
        "p_brand1", {hustle::catalog::HustleType::CHAR, 9}, true, false);
    SQLTest::part.addColumn(p_partkey);
    SQLTest::part.addColumn(p_mfgr);
    SQLTest::part.addColumn(p_category);
    SQLTest::part.addColumn(p_brand1);
    SQLTest::part.setPrimaryKey({});

    // Create table supplier
    // hustle::catalog::TableSchema supplier("supplier");
    hustle::catalog::ColumnSchema s_suppkey(
        "s_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema s_city(
        "s_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
    hustle::catalog::ColumnSchema s_nation(
        "s_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
    hustle::catalog::ColumnSchema s_region(
        "s_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);

    SQLTest::supplier.addColumn(s_suppkey);
    SQLTest::supplier.addColumn(s_city);
    SQLTest::supplier.addColumn(s_nation);
    SQLTest::supplier.addColumn(s_region);
    SQLTest::supplier.setPrimaryKey({});

    // Create table customer
    // hustle::catalog::TableSchema customer("customer");
    hustle::catalog::ColumnSchema c_suppkey(
        "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema c_city(
        "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
    hustle::catalog::ColumnSchema c_nation(
        "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
    hustle::catalog::ColumnSchema c_region(
        "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
    SQLTest::customer.addColumn(c_suppkey);
    SQLTest::customer.addColumn(c_city);
    SQLTest::customer.addColumn(c_nation);
    SQLTest::customer.addColumn(c_region);
    SQLTest::customer.setPrimaryKey({});

    // Create table ddate
    // hustle::catalog::TableSchema ddate("ddate");
    hustle::catalog::ColumnSchema d_datekey(
        "d_datekey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema d_dayofweek(
        "d_dayofweek", {hustle::catalog::HustleType::CHAR, 10}, true, false);
    hustle::catalog::ColumnSchema d_month(
        "d_month", {hustle::catalog::HustleType::CHAR, 10}, true, false);
    hustle::catalog::ColumnSchema d_year(
        "d_year", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema d_weeknuminyear(
        "d_weeknuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
        false);
    hustle::catalog::ColumnSchema d_yearmonthnum(
        "d_yearmonthnum", {hustle::catalog::HustleType::INTEGER, 0}, true,
        false);
    hustle::catalog::ColumnSchema d_yearmonth(
        "d_yearmonth", {hustle::catalog::HustleType::CHAR, 8}, true, false);

    SQLTest::ddate.addColumn(d_datekey);
    SQLTest::ddate.addColumn(d_dayofweek);
    SQLTest::ddate.addColumn(d_month);
    SQLTest::ddate.addColumn(d_year);
    SQLTest::ddate.addColumn(d_weeknuminyear);
    SQLTest::ddate.addColumn(d_yearmonthnum);
    SQLTest::ddate.addColumn(d_yearmonth);
    SQLTest::ddate.setPrimaryKey({});

    // Create table lineorder
    // hustle::catalog::TableSchema lineorder("lineorder");
    hustle::catalog::ColumnSchema lo_orderkey(
        "lo_orderkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_linenumber(
        "lo_linenumber", {hustle::catalog::HustleType::INTEGER, 0}, true,
        false);
    hustle::catalog::ColumnSchema lo_custkey(
        "lo_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_partkey(
        "lo_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_suppkey(
        "lo_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_orderdate(
        "lo_orderdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_orderpriority(
        "lo_orderpriority", {hustle::catalog::HustleType::CHAR, 15}, true,
        false);
    hustle::catalog::ColumnSchema lo_quantity(
        "lo_quantity", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_revenue(
        "lo_revenue", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema lo_extendedprice(
        "lo_extendedprice", {hustle::catalog::HustleType::INTEGER, 0}, true,
        false);
    hustle::catalog::ColumnSchema lo_discount(
        "lo_discount", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

    SQLTest::lineorder.addColumn(lo_orderkey);
    SQLTest::lineorder.addColumn(lo_linenumber);
    SQLTest::lineorder.addColumn(lo_custkey);
    SQLTest::lineorder.addColumn(lo_partkey);
    SQLTest::lineorder.addColumn(lo_suppkey);
    SQLTest::lineorder.addColumn(lo_orderdate);
    SQLTest::lineorder.addColumn(lo_quantity);
    SQLTest::lineorder.addColumn(lo_revenue);
    SQLTest::lineorder.addColumn(lo_extendedprice);
    SQLTest::lineorder.addColumn(lo_discount);
    SQLTest::lineorder.setPrimaryKey({});

    SQLTest::lo = std::make_shared<hustle::storage::DBTable>(
        "lineorder", SQLTest::lineorder.getArrowSchema(), BLOCK_SIZE);
    SQLTest::c = std::make_shared<hustle::storage::DBTable>(
        "customer", SQLTest::customer.getArrowSchema(), BLOCK_SIZE);
    SQLTest::s = std::make_shared<hustle::storage::DBTable>(
        "supplier", SQLTest::supplier.getArrowSchema(), BLOCK_SIZE);
    SQLTest::p = std::make_shared<hustle::storage::DBTable>(
        "part", SQLTest::part.getArrowSchema(), BLOCK_SIZE);
    SQLTest::d = std::make_shared<hustle::storage::DBTable>(
        "ddate", SQLTest::ddate.getArrowSchema(), BLOCK_SIZE);

    hustle_db->create_table(SQLTest::lineorder, SQLTest::lo);
    hustle_db->create_table(SQLTest::customer, SQLTest::c);
    hustle_db->create_table(SQLTest::supplier, SQLTest::s);
    hustle_db->create_table(SQLTest::part, SQLTest::p);
    hustle_db->create_table(SQLTest::ddate, SQLTest::d);

    std::cerr << "Create Table " << std::endl;
    FILE* stream = fopen(LINE_ORDER_PATH, "r");
    char line[2048];
    std::string query = "BEGIN TRANSACTION;";
    int count = 0;
    while (fgets(line, 2048, stream)) {
      char* tmp = strdup(line);
      char** fields = getfields(tmp, 10);
      query += "INSERT INTO lineorder VALUES (" +
               StringUtils::trim(std::string(fields[0])) + ", " +
               StringUtils::trim(std::string(fields[1])) + ", " +
               StringUtils::trim(std::string(fields[2])) + ", " +
               StringUtils::trim(std::string(fields[3])) +
               ","
               "" +
               StringUtils::trim(std::string(fields[4])) + ", '" +
               StringUtils::trim(std::string(fields[5])) + "', " +
               StringUtils::trim(std::string(fields[6])) + ", " +
               StringUtils::trim(std::string(fields[7])) + ", " +
               StringUtils::trim(std::string(fields[8])) + "," +
               StringUtils::trim(std::string(fields[9])) + ");";
      count++;
      if (count == 10000) {
        query += "COMMIT;";
        hustle_db->execute_query_result(query);
        query = "BEGIN TRANSACTION;";
      }
    }
    if (count != 2000) {
      query += "COMMIT;";
      hustle_db->execute_query_result(query);
    }

    std::cerr << "lineorder done" << std::endl;

    stream = fopen(PART_PATH, "r");
    query = "BEGIN TRANSACTION;";
    while (fgets(line, 2048, stream)) {
      char* tmp = strdup(line);
      char** fields = getfields(tmp, 4);
      query += "INSERT INTO part VALUES (" +
               StringUtils::trim(std::string(fields[0])) + ", '" +
               StringUtils::trim(std::string(fields[1])) + "', '" +
               StringUtils::trim(std::string(fields[2])) + "', '" +
               StringUtils::trim(std::string(fields[3])) + "');\n";
    }
    query += "COMMIT;";
    hustle_db->execute_query_result(query);
    std::cerr << "part done" << std::endl;

    stream = fopen(SUPPLIER_PATH, "r");
    query = "BEGIN TRANSACTION;";
    while (fgets(line, 2048, stream)) {
      char* tmp = strdup(line);
      // std::cout << line << std::endl;
      char** fields = getfields(tmp, 4);
      query += "INSERT INTO supplier VALUES (" +
               StringUtils::trim(std::string(fields[0])) + ", '" +
               StringUtils::trim(std::string(fields[1])) + "', '" +
               StringUtils::trim(std::string(fields[2])) + "', '" +
               StringUtils::trim(std::string(fields[3])) + "');\n";
    }
    query += "COMMIT;";
    hustle_db->execute_query_result(query);
    std::cerr << "supplier done" << std::endl;

    stream = fopen(CUSTOMER_PATH, "r");
    query = "BEGIN TRANSACTION;";
    while (fgets(line, 2048, stream)) {
      char* tmp = strdup(line);
      char** fields = getfields(tmp, 17);
      query += "INSERT INTO customer VALUES (" +
               StringUtils::trim(std::string(fields[0])) + ", '" +
               StringUtils::trim(std::string(fields[1])) + "', '" +
               StringUtils::trim(std::string(fields[2])) + "', '" +
               StringUtils::trim(std::string(fields[3])) + "');\n";
    }
    query += "COMMIT;";
    hustle_db->execute_query_result(query);
    std::cerr << "customer done" << std::endl;

    stream = fopen(DATE_PATH, "r");
    query = "BEGIN TRANSACTION;";
    while (fgets(line, 2048, stream)) {
      char* tmp = strdup(line);
      char** fields = getfields(tmp, 7);
      query += "INSERT INTO ddate VALUES (" +
               StringUtils::trim(std::string(fields[0])) + ", '" +
               StringUtils::trim(std::string(fields[1])) + "', '" +
               StringUtils::trim(std::string(fields[2])) + "'," +
               StringUtils::trim(std::string(fields[3])) + ", " +
               StringUtils::trim(std::string(fields[4])) + ", " +
               StringUtils::trim(std::string(fields[5])) + "," + "'" +
               StringUtils::trim(std::string(fields[6])) + "');\n";
    }
    query += "COMMIT;";
    hustle_db->execute_query_result(query);
    std::cerr << "date done" << std::endl;

    hustle::HustleDB::start_scheduler();
  }

  void TearDown() override { hustle::HustleDB::stop_scheduler(); }
};

hustle::catalog::TableSchema SQLTest::part("part"),
    SQLTest::supplier("supplier"), SQLTest::customer("customer"),
    SQLTest::ddate("ddate"), SQLTest::lineorder("lineorder");

DBTable::TablePtr SQLTest::lo, SQLTest::d, SQLTest::p, SQLTest::c, SQLTest::s;

std::shared_ptr<hustle::HustleDB> SQLTest::hustle_db;

TEST_F(SQLTest, q1_join_reorder) {
  std::string query =
      "select sum(lo_extendedprice*lo_discount) as "
      "revenue "
      "from lineorder, ddate "
      "where d_datekey = lo_orderdate and d_year = 1993 and (lo_discount "
      "BETWEEN 0 and 3 and lo_quantity < 25);";
  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "3249196\n");
}

TEST_F(SQLTest, q2) {
  std::string query =
      "select sum(lo_extendedprice * lo_discount) as "
      "revenue\n"
      "from lineorder, ddate\n"
      "where d_datekey = lo_orderdate\n"
      "and (lo_discount BETWEEN 5 and 7\n"
      "and lo_quantity BETWEEN 26 and 35);";
  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "40072918\n");
}

TEST_F(SQLTest, q3) {
  std::string query =
      "select sum(lo_extendedprice) as "
      "revenue\n"
      "from lineorder, ddate\n"
      "where d_datekey = lo_orderdate\n"
      "and (d_weeknuminyear = 6 and d_year = 1994)\n"
      "and (lo_discount < 50\n"
      "and lo_quantity < 50);";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "18047544\n");
}