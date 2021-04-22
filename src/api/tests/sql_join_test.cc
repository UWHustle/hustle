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
public:
    static hustle::catalog::TableSchema t1_schema, t2_schema;
    static DBTable::TablePtr t1, t2;
    static std::shared_ptr<hustle::HustleDB> hustle_db;

    void SetUp() override {
        std::filesystem::remove_all("db_directory_join_test");
        EXPECT_FALSE(std::filesystem::exists("db_directory_join_test"));

        SQLJoinTest::hustle_db = std::make_shared<hustle::HustleDB>("db_directory_join_test");

        // Create table part
        hustle::catalog::ColumnSchema a (
                "a", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema b (
                "b", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        hustle::catalog::ColumnSchema c (
                "c", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        SQLJoinTest::t1_schema.addColumn(a);
        SQLJoinTest::t1_schema.addColumn(b);
        SQLJoinTest::t1_schema.addColumn(c);

        hustle::catalog::ColumnSchema d (
                "d", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

        SQLJoinTest::t2_schema.addColumn(b);
        SQLJoinTest::t2_schema.addColumn(c);
        SQLJoinTest::t2_schema.addColumn(d);

        SQLJoinTest::t1 = std::make_shared<hustle::storage::DBTable>(
                "t1", SQLJoinTest::t1_schema.getArrowSchema(), BLOCK_SIZE);
        SQLJoinTest::t2 = std::make_shared<hustle::storage::DBTable>(
                "t2", SQLJoinTest::t2_schema.getArrowSchema(), BLOCK_SIZE);

        hustle_db->create_table(SQLJoinTest::t1_schema, SQLJoinTest::t1);
        hustle_db->create_table(SQLJoinTest::t2_schema, SQLJoinTest::t2);

        hustle_db->execute_query_result("INSERT INTO t1 VALUES(1,2,3);");
        hustle_db->execute_query_result("INSERT INTO t1 VALUES(2,3,4);");
        hustle_db->execute_query_result("INSERT INTO t1 VALUES(3,4,5);");

        hustle_db->execute_query_result("INSERT INTO t2 VALUES(1,2,3);");
        hustle_db->execute_query_result("INSERT INTO t2 VALUES(2,3,4);");
        hustle_db->execute_query_result("INSERT INTO t2 VALUES(3,4,5);");

        hustle::HustleDB::init();
    }


    void TearDown() override {
        hustle::HustleDB::destroy();
        SQLJoinTest::t1.reset();
        SQLJoinTest::t2.reset();
        hustle_db.reset();
        std::filesystem::remove_all("db_directory_join_test");
    }
};

hustle::catalog::TableSchema SQLJoinTest::t1_schema("t1"),
        SQLJoinTest::t2_schema("t2");

DBTable::TablePtr SQLJoinTest::t1, SQLJoinTest::t2;

std::shared_ptr<hustle::HustleDB> SQLJoinTest::hustle_db;


TEST_F(SQLJoinTest, q1_join) {
    std::string query =
    "SELECT t1.rowid, t2.rowid, '|' FROM t1, t2 ON t1.a=t2.b;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 1 | |\n2 | 2 | |\n3 | 3 | |\n");
}

TEST_F(SQLJoinTest, q2_join) {
    std::string query =
            "SELECT * FROM t1 NATURAL JOIN t2;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | 4\n2 | 3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q3_join) {
    std::string query =
            "SELECT * FROM t1 INNER JOIN t2 USING(b,c);";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | 4\n2 | 3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q4_join) {
    std::string query =
            "SELECT t1.* FROM t1 NATURAL LEFT JOIN t2;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3\n2 | 3 | 4\n3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q5_join) {
    std::string query =
            "SELECT * FROM t2 NATURAL LEFT OUTER JOIN t1;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | NULL\n2 | 3 | 4 | 1\n3 | 4 | 5 | 2\n");
}

TEST_F(SQLJoinTest, q6_join) {
    std::string query =
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output,  "2 | 3 | 4 | NULL | NULL | NULL\n3 | 4 | 5 | 1 | 2 | 3\n");
}

TEST_F(SQLJoinTest, q7_join) {
    std::string query =
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t2.b IS NULL OR t2.b>1";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | NULL | NULL | NULL\n2 | 3 | 4 | NULL | NULL | NULL\n");
}

TEST_F(SQLJoinTest, q8_join) {
    std::string query =
            "SELECT * FROM t1 CROSS JOIN t2 USING(b,c);";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | 4\n2 | 3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q9_join) {
    std::string query =
            "SELECT * FROM t1 NATURAL INNER JOIN t2;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | 4\n2 | 3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q10_join) {
    std::string query =
            " SELECT * FROM t1 NATURAL JOIN \n"
            "        (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as t3;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1 | 2 | 3 | 4 | 5\n");
}

TEST_F(SQLJoinTest, q11_join) {
    std::string query =
    "SELECT * FROM (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as 'tx'"
    "NATURAL JOIN t1;";
    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output,   "3 | 4 | 5 | 1 | 2\n");
}
