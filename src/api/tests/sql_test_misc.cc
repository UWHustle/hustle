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

class SQLMiscTest : public SQLTest {};

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

TEST_F(SQLMiscTest, q2_join_reorder) {
  std::string query =
      "select sum(lo_extendedprice * lo_discount) as "
      "revenue\n"
      "from lineorder, ddate\n"
      "where d_datekey = lo_orderdate\n"
      "and (lo_discount BETWEEN 5 and 7\n"
      "and lo_quantity BETWEEN 26 and 35);";
  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "20036459\n");
}

TEST_F(SQLMiscTest, q3_join_reorder) {
  std::string query =
      "select sum(lo_extendedprice) as "
      "revenue\n"
      "from lineorder, ddate\n"
      "where d_datekey = lo_orderdate\n"
      "and (d_weeknuminyear = 6 and d_year = 1994)\n"
      "and (lo_discount < 50\n"
      "and lo_quantity < 50);";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "6015848\n");
}

TEST_F(SQLMiscTest, q_without_agg_and_join) {
  std::string query =
      "select d_month, d_weeknuminyear, d_dayofweek, d_yearmonth\n"
      "from ddate\n"
      "where d_datekey = 1992015\n;";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "5 | 5 | 5 | Jan1992\n");
}

TEST_F(SQLMiscTest, q_without_agg) {
  std::string query =
      "select  lo_revenue, lo_quantity, d_month, d_yearmonth\n"
      "from ddate, lineorder\n"
      "where d_datekey = lo_orderdate  and (lo_quantity = 29 and lo_revenue = "
      "946)\n;";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output,
            "946 | 29 | 4 | Feb1992\n"
            "946 | 29 | 4 | Mar1992\n");
}

TEST_F(SQLMiscTest, q_without_join) {
  std::string query =
      "select Count(lo_quantity)\n"
      "from  lineorder\n"
      "where (lo_quantity = 29 and lo_revenue = 946)\n;";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "2\n");
}

TEST_F(SQLMiscTest, q_predicate_assorted) {
    std::string query =
            "select Count(lo_quantity)\n"
            "from  lineorder, ddate\n"
            "where d_datekey = lo_orderdate and d_year = 1993 and (lo_quantity = 20 and lo_revenue = "
            "763);\n";

    std::string output = hustle_db->execute_query_result(query);
    EXPECT_EQ(output, "1\n");
}


TEST_F(SQLMiscTest, q_joins_non_unique_columns) {
  std::string query =
      "select lo_orderkey, d_datekey, d_dayofweek\n"
      "from  lineorder, ddate\n"
      "where d_dayofweek = lo_custkey and (lo_quantity = 20 and lo_revenue = "
      "763);\n";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(
      output,
      "6 | 1992011 | 1\n6 | 1993121 | 1\n6 | 1994021 | 1\n6 | 1994031 | 1\n6 | "
      "1992018 | 1\n6 | 1993128 | 1\n6 | 1994028 | 1\n6 | 1994038 | 1\n6 | "
      "19920115 | 1\n6 | 19931215 | 1\n6 | 19940215 | 1\n6 | 19940315 | 1\n6 | "
      "19920122 | 1\n6 | 19931222 | 1\n6 | 19940222 | 1\n6 | 19940322 | 1\n6 | "
      "19920129 | 1\n6 | 19931229 | 1\n6 | 19940229 | 1\n6 | 19940329 | 1\n");

  query =
      "select Count(d_dayofweek)\n"
      "from  ddate\n"
      "where d_dayofweek = 1;\n";

  output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "20\n");
}

TEST_F(SQLMiscTest, q_agg_non_supported) {
  std::string query =
      "select Max(lo_orderkey)\n"
      "from  lineorder\n"
      "where (lo_quantity = 29 or lo_revenue = 946)\n;";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "19998\n");
}

TEST_F(SQLMiscTest, q_nested_subquery_predicate) {
  std::string query =
      "SELECT lo_orderkey\n"
      "FROM  lineorder\n"
      "WHERE lo_orderkey < (\n"
      "SELECT lo_orderkey FROM lineorder WHERE lo_orderkey=3);\n";

  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "0\n1\n2\n");

  query =
      "SELECT lo_orderkey\n"
      "FROM  lineorder\n"
      "WHERE lo_orderkey IN (\n"
      "SELECT lo_orderkey FROM lineorder WHERE lo_orderkey < 5);\n";

  output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "0\n1\n2\n3\n4\n");

  query =
      "SELECT lo_orderkey\n"
      "FROM  lineorder\n"
      "WHERE EXISTS (\n"
      "SELECT lo_orderkey FROM lineorder WHERE lo_orderkey=-1);\n";

  output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "");
}

TEST_F(SQLMiscTest, q_nested_query_from) {
  std::string query =
      "select Avg(lineorder_stat.t_revenue)\n"
      "from ( select SUM(lo_revenue) as t_revenue from lineorder group by "
      "lo_quantity ) as lineorder_stat;\n;";
  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "323972.225806452\n");
}

TEST_F(SQLMiscTest, q_unsupported_having) {
  std::string query =
      "select lo_quantity, Count(lo_revenue)\n"
      "from  lineorder\n"
      "group by lo_quantity\n"
      "having Count(lo_revenue) > 2200\n;";
  std::string output = hustle_db->execute_query_result(query);
  EXPECT_EQ(output, "25 | 2247\n26 | 2271\n27 | 2264\n28 | 2215\n29 | 2291\n");
}