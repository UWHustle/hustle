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

#include "gtest/gtest.h"
#include "sql_test.cc"
#include "sqlite3/sqlite3.h"
#include "utils/string_utils.h"

using namespace testing;
using namespace hustle::resolver;

class SQLJoinTest : public SQLTest {

    static hustle::catalog::TableSchema t1_schema, t2_schema;
    static DBTable::TablePtr t1, t2;
    static std::shared_ptr<hustle::HustleDB> hustle_db;

    void SetUp() override {
        int num_remove = std::filesystem::remove_all("db_directory_join");
        EXPECT_FALSE(std::filesystem::exists("db_directory_join"));

        SQLTest::hustle_db = std::make_shared<hustle::HustleDB>("db_directory_join");

        // Create table part
        //hustle::catalog::TableSchema part("part");
        hustle::catalog::ColumnSchema a (
                "a", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema b (
                "b", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        hustle::catalog::ColumnSchema c (
                "c", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        SQLTest::t1_schema.addColumn(a);
        SQLTest::t1_schema.addColumn(b);
        SQLTest::t1_schema.addColumn(c);

        // Create table supplier
        // hustle::catalog::TableSchema supplier("supplier");
        hustle::catalog::ColumnSchema b (
                "b", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema c (
                "c", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        hustle::catalog::ColumnSchema d (
                "d", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        SQLTest::t2_schema.addColumn(b);
        SQLTest::t2_schema.addColumn(c);
        SQLTest::t2_schema.addColumn(d);

        SQLTest::t1 = std::make_shared<hustle::storage::DBTable>(
                "t1", SQLTest::t1_schema.getArrowSchema(), BLOCK_SIZE);
        SQLTest::t2 = std::make_shared<hustle::storage::DBTable>(
                "t2", SQLTest::t2_schema.getArrowSchema(), BLOCK_SIZE);

        hustle_db->create_table(SQLTest::t1_schema, SQLTest::t1);
        hustle_db->create_table(SQLTest::t2_schema, SQLTest::t2);

        hustle_db->execute_query_result("INSERT INTO t1 VALUES(1,2,3);");
        hustle_db->execute_query_result("INSERT INTO t1 VALUES(2,3,4);");
        hustle_db->execute_query_result("INSERT INTO t1 VALUES(3,4,5);");

        hustle_db->execute_query_result("INSERT INTO t2 VALUES(1,2,3);");
        hustle_db->execute_query_result("INSERT INTO t2 VALUES(2,3,4);");
        hustle_db->execute_query_result("INSERT INTO t2 VALUES(3,4,5);");

        hustle::HustleDB::init();
    }
};


TEST_F(SQLMiscTest, q1_join_reorder) {
    std::string query =
            "select sum(lo_extendedprice*lo_discount) as "
            "revenue "
            "from lineorder, ddate "
            "where d_datekey = lo_orderdate and d_year = 1993 and (lo_discount "
            "BETWEEN 0 and 3 and lo_quantity < 25);";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "3249196\n");
}