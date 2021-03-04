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
#include "utils/sqlite_utils_parse.h"

using namespace testing;
using namespace hustle::resolver;

class SQLTest : public Test {
public:
    static hustle::catalog::TableSchema part, supplier, customer, ddate,
            lineorder;
    static DBTable::TablePtr lo, d, p, c, s;
    static std::shared_ptr<hustle::HustleDB> hustle_db;
    void SetUp() override {
        /**
          CREATE TABLE part
          (
            p_partkey     INTEGER NOT NULL,
            p_name        VARCHAR(22) NOT NULL,
            p_mfgr        VARCHAR(6) NOT NULL,
            p_category    VARCHAR(7) NOT NULL,
            p_brand1      VARCHAR(9) NOT NULL,
            p_color       VARCHAR(11) NOT NULL,
            p_type        VARCHAR(25) NOT NULL,
            p_size        INTEGER NOT NULL,
            p_container   VARCHAR(10) NOT NULL
          );
          CREATE TABLE supplier
          (
            s_suppkey   INTEGER NOT NULL,
            s_name      VARCHAR(25) NOT NULL,
            s_address   VARCHAR(25) NOT NULL,
            s_city      VARCHAR(10) NOT NULL,
            s_nation    VARCHAR(15) NOT NULL,
            s_region    VARCHAR(12) NOT NULL,
            s_phone     VARCHAR(15) NOT NULL
          );
          CREATE TABLE customer
          (
            c_custkey      INTEGER NOT NULL,
            c_name         VARCHAR(25) NOT NULL,
            c_address      VARCHAR(25) NOT NULL,
            c_city         VARCHAR(10) NOT NULL,
            c_nation       VARCHAR(15) NOT NULL,
            c_region       VARCHAR(12) NOT NULL,
            c_phone        VARCHAR(15) NOT NULL,
            c_mktsegment   VARCHAR(10) NOT NULL
          );
          CREATE TABLE ddate
          (
            d_datekey            INTEGER NOT NULL,
            d_date               VARCHAR(19) NOT NULL,
            d_dayofweek          VARCHAR(10) NOT NULL,
            d_month              VARCHAR(10) NOT NULL,
            d_year               INTEGER NOT NULL,
            d_yearmonthnum       INTEGER NOT NULL,
            d_yearmonth          VARCHAR(8) NOT NULL,
            d_daynuminweek       INTEGER NOT NULL,
            d_daynuminmonth      INTEGER NOT NULL,
            d_daynuminyear       INTEGER NOT NULL,
            d_monthnuminyear     INTEGER NOT NULL,
            d_weeknuminyear      INTEGER NOT NULL,
            d_sellingseason      VARCHAR(13) NOT NULL,
            d_lastdayinweekfl    VARCHAR(1) NOT NULL,
            d_lastdayinmonthfl   VARCHAR(1) NOT NULL,
            d_holidayfl          VARCHAR(1) NOT NULL,
            d_weekdayfl          VARCHAR(1) NOT NULL
          );
          CREATE TABLE lineorder
          (
            lo_orderkey          INTEGER NOT NULL,
            lo_linenumber        INTEGER NOT NULL,
            lo_custkey           INTEGER NOT NULL,
            lo_partkey           INTEGER NOT NULL,
            lo_suppkey           INTEGER NOT NULL,
            lo_orderdate         INTEGER NOT NULL,
            lo_orderpriority     VARCHAR(15) NOT NULL,
            lo_shippriority      VARCHAR(1) NOT NULL,
            lo_quantity          INTEGER NOT NULL,
            lo_extendedprice     INTEGER NOT NULL,
            lo_ordertotalprice   INTEGER NOT NULL,
            lo_discount          INTEGER NOT NULL,
            lo_revenue           INTEGER NOT NULL,
            lo_supplycost        INTEGER NOT NULL,
            lo_tax               INTEGER NOT NULL,
            lo_commitdate        INTEGER NOT NULL,
            lo_shipmode          VARCHAR(10) NOT NULL
          );
         */

        std::filesystem::remove_all("db_directory");
        EXPECT_FALSE(std::filesystem::exists("db_directory"));

        SQLTest::hustle_db = std::make_shared<hustle::HustleDB>("db_directory66");

        // Create table part
        // hustle::catalog::TableSchema part("part");
        hustle::catalog::ColumnSchema p_partkey(
                "p_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_name(
                "p_name", {hustle::catalog::HustleType::CHAR, 22}, true, false);
        hustle::catalog::ColumnSchema p_mfgr(
                "p_mfgr", {hustle::catalog::HustleType::CHAR, 6}, true, false);
        hustle::catalog::ColumnSchema p_category(
                "p_category", {hustle::catalog::HustleType::CHAR, 7}, true, false);
        hustle::catalog::ColumnSchema p_brand1(
                "p_brand1", {hustle::catalog::HustleType::CHAR, 9}, true, false);
        hustle::catalog::ColumnSchema p_color(
                "p_color", {hustle::catalog::HustleType::CHAR, 11}, true, false);
        hustle::catalog::ColumnSchema p_type(
                "p_type", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema p_size(
                "p_size", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_container(
                "p_container", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        SQLTest::part.addColumn(p_partkey);
        SQLTest::part.addColumn(p_name);
        SQLTest::part.addColumn(p_mfgr);
        SQLTest::part.addColumn(p_category);
        SQLTest::part.addColumn(p_brand1);
        SQLTest::part.addColumn(p_color);
        SQLTest::part.addColumn(p_type);
        SQLTest::part.addColumn(p_size);
        SQLTest::part.addColumn(p_container);
        SQLTest::part.setPrimaryKey({});

        // Create table supplier
        // hustle::catalog::TableSchema supplier("supplier");
        hustle::catalog::ColumnSchema s_suppkey(
                "s_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema s_name(
                "s_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_address(
                "s_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_city(
                "s_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema s_nation(
                "s_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema s_region(
                "s_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
        hustle::catalog::ColumnSchema s_phone(
                "s_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        SQLTest::supplier.addColumn(s_suppkey);
        SQLTest::supplier.addColumn(s_name);
        SQLTest::supplier.addColumn(s_address);
        SQLTest::supplier.addColumn(s_city);
        SQLTest::supplier.addColumn(s_nation);
        SQLTest::supplier.addColumn(s_region);
        SQLTest::supplier.addColumn(s_phone);
        SQLTest::supplier.setPrimaryKey({});

        // Create table customer
        // hustle::catalog::TableSchema customer("customer");
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
        SQLTest::customer.addColumn(c_suppkey);
        SQLTest::customer.addColumn(c_name);
        SQLTest::customer.addColumn(c_address);
        SQLTest::customer.addColumn(c_city);
        SQLTest::customer.addColumn(c_nation);
        SQLTest::customer.addColumn(c_region);
        SQLTest::customer.addColumn(c_phone);
        SQLTest::customer.addColumn(c_mktsegment);
        SQLTest::customer.setPrimaryKey({});

        // Create table ddate
        // hustle::catalog::TableSchema ddate("ddate");
        hustle::catalog::ColumnSchema d_datekey(
                "d_datekey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_date(
                "d_date", {hustle::catalog::HustleType::CHAR, 19}, true, false);
        hustle::catalog::ColumnSchema d_dayofweek(
                "d_dayofweek", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_month(
                "d_month", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_year(
                "d_year", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_yearmonthnum(
                "d_yearmonthnum", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_yearmonth(
                "d_yearmonth", {hustle::catalog::HustleType::CHAR, 8}, true, false);
        hustle::catalog::ColumnSchema d_daynuminweek(
                "d_daynuminweek", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_daynuminmonth(
                "d_daynuminmonth", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_daynuminyear(
                "d_daynuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_monthnuminyear(
                "d_monthnuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_weeknuminyear(
                "d_weeknuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_sellingseason(
                "d_sellingseason", {hustle::catalog::HustleType::CHAR, 13}, true,
                false);
        hustle::catalog::ColumnSchema d_lastdayinweekfl(
                "d_lastdayinweekfl", {hustle::catalog::HustleType::CHAR, 1}, true,
                false);
        hustle::catalog::ColumnSchema d_lastdayinmonthfl(
                "d_lastdayinmonthfl", {hustle::catalog::HustleType::CHAR, 1}, true,
                false);
        hustle::catalog::ColumnSchema d_holidayfl(
                "d_holidayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_weekdayfl(
                "d_weekdayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        SQLTest::ddate.addColumn(d_datekey);
        SQLTest::ddate.addColumn(d_date);
        SQLTest::ddate.addColumn(d_dayofweek);
        SQLTest::ddate.addColumn(d_month);
        SQLTest::ddate.addColumn(d_year);
        SQLTest::ddate.addColumn(d_yearmonthnum);
        SQLTest::ddate.addColumn(d_yearmonth);
        SQLTest::ddate.addColumn(d_daynuminweek);
        SQLTest::ddate.addColumn(d_daynuminmonth);
        SQLTest::ddate.addColumn(d_daynuminyear);
        SQLTest::ddate.addColumn(d_monthnuminyear);
        SQLTest::ddate.addColumn(d_weeknuminyear);
        SQLTest::ddate.addColumn(d_sellingseason);
        SQLTest::ddate.addColumn(d_lastdayinweekfl);
        SQLTest::ddate.addColumn(d_lastdayinmonthfl);
        SQLTest::ddate.addColumn(d_holidayfl);
        SQLTest::ddate.addColumn(d_weekdayfl);
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
        hustle::catalog::ColumnSchema lo_shippriority(
                "lo_shippriority", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema lo_quantity(
                "lo_quantity", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_extendedprice(
                "lo_extendedprice", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_ordertotalprice(
                "lo_ordertotalprice", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_discount(
                "lo_discount", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_revenue(
                "lo_revenue", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_supplycost(
                "lo_supplycost", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_tax(
                "lo_tax", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_commitdate(
                "lo_commitdate", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_shipmode(
                "lo_shipmode", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        SQLTest::lineorder.addColumn(lo_orderkey);
        SQLTest::lineorder.addColumn(lo_linenumber);
        SQLTest::lineorder.addColumn(lo_custkey);
        SQLTest::lineorder.addColumn(lo_partkey);
        SQLTest::lineorder.addColumn(lo_suppkey);
        SQLTest::lineorder.addColumn(lo_orderdate);
        SQLTest::lineorder.addColumn(lo_orderpriority);
        SQLTest::lineorder.addColumn(lo_shippriority);
        SQLTest::lineorder.addColumn(lo_quantity);
        SQLTest::lineorder.addColumn(lo_extendedprice);
        SQLTest::lineorder.addColumn(lo_ordertotalprice);
        SQLTest::lineorder.addColumn(lo_discount);
        SQLTest::lineorder.addColumn(lo_revenue);
        SQLTest::lineorder.addColumn(lo_supplycost);
        SQLTest::lineorder.addColumn(lo_tax);
        SQLTest::lineorder.addColumn(lo_commitdate);
        SQLTest::lineorder.addColumn(lo_shipmode);
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

        hustle_db->createTable(SQLTest::lineorder, SQLTest::lo);
        hustle_db->createTable(SQLTest::customer, SQLTest::c);
        hustle_db->createTable(SQLTest::supplier, SQLTest::s);
        hustle_db->createTable(SQLTest::part, SQLTest::p);
        hustle_db->createTable(SQLTest::ddate, SQLTest::d);
    }


};

hustle::catalog::TableSchema SQLTest::part("part"),
        SQLTest::supplier("supplier"), SQLTest::customer("customer"),
        SQLTest::ddate("ddate"), SQLTest::lineorder("lineorder");

DBTable::TablePtr SQLTest::lo, SQLTest::d, SQLTest::p,
        SQLTest::c, SQLTest::s;

std::shared_ptr<hustle::HustleDB> SQLTest::hustle_db;

TEST_F(SQLTest, q1) {
    std::string query =
            "select sum(lo_extendedprice) as "
            "revenue "
            "from lineorder, ddate "
            "where lo_orderdate = d_datekey and d_year = 1993 and lo_discount "
            "< 3 and lo_quantity < 25;";


    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 1);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 0);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 0);

    EXPECT_EQ(select_resolver.project_references()->size(), 1);
}

TEST_F(SQLTest, q2) {
    std::string query =
            "select sum(lo_extendedprice) as "
            "revenue\n"
            "from lineorder, ddate\n"
            "where lo_orderdate = d_datekey\n"
            "and d_yearmonthnum = 199401\n"
            "and lo_discount < 6\n"
            "and lo_quantity < 35;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 1);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 0);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 0);

    EXPECT_EQ(select_resolver.project_references()->size(), 1);
}

TEST_F(SQLTest, q3) {
    std::string query =
            "select sum(lo_extendedprice) as "
            "revenue\n"
            "from lineorder, ddate\n"
            "where lo_orderdate = d_datekey\n"
            "and d_weeknuminyear = 6 and d_year = 1994\n"
            "and lo_discount < 7\n"
            "and lo_quantity < 40;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 1);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 0);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 0);

    EXPECT_EQ(select_resolver.project_references()->size(), 1);
}

TEST_F(SQLTest, q4) {
    std::string query =
            "select sum(lo_revenue), d_year, p_brand1\n"
            "from lineorder, ddate, part, supplier\n"
            "where lo_partkey = p_partkey\n"
            "and lo_suppkey = s_suppkey\n"
            "and lo_orderdate = d_datekey\n"
            "and p_category = 'MFGR#12'\n"
            "and s_region = 'AMERICA'\n"
            "group by d_year, p_brand1\n"
            "order by d_year, p_brand1;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 2);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 3);
}

TEST_F(SQLTest, q5) {
    std::string query =
            "select sum(lo_revenue), d_year, p_brand1\n"
            "\tfrom lineorder, ddate, part, supplier\n"
            "\twhere lo_partkey = p_partkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand p_brand1 > 'MFGR#2221' and p_brand1 < 'MFGR#2228'\n"
            "\t\tand s_region = 'ASIA'\n"
            "\tgroup by d_year, p_brand1\n"
            "\torder by d_year, p_brand1;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 2);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 3);
}

TEST_F(SQLTest, q6) {
    std::string query =
            "select sum(lo_revenue), d_year, p_brand1\n"
            "\tfrom lineorder, ddate, part, supplier\n"
            "\twhere lo_partkey = p_partkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand p_brand1 = 'MFGR#2221'\n"
            "\t\tand s_region = 'EUROPE'\n"
            "\tgroup by d_year, p_brand1\n"
            "\torder by d_year, p_brand1;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 2);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 3);
}

TEST_F(SQLTest, q7) {
    std::string query =
            "select c_nation, s_nation, d_year, sum(lo_revenue) "
            "as revenue\n"
            "\tfrom customer, lineorder, supplier, ddate\n"
            "\twhere lo_custkey = c_custkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand c_region = 'ASIA'\n"
            "\t\tand s_region = 'ASIA'\n"
            "\t\tand d_year >= 1992 and d_year <= 1997\n"
            "\tgroup by c_nation, s_nation, d_year\n"
            "\torder by d_year asc, revenue desc;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, q8) {
    std::string query =
            "select c_city, s_city, d_year, sum(lo_revenue) as "
            "revenue\n"
            "\tfrom customer, lineorder, supplier, ddate\n"
            "\twhere lo_custkey = c_custkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand c_nation = 'UNITED STATES'\n"
            "\t\tand s_nation = 'UNITED STATES'\n"
            "\t\tand d_year >= 1992 and d_year <= 1997\n"
            "\tgroup by c_city, s_city, d_year\n"
            "\torder by d_year asc, revenue desc;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, q9) {
    std::string query =
            "select c_city, s_city, d_year, sum(lo_revenue) as "
            "revenue\n"
            "\tfrom customer, lineorder, supplier, ddate\n"
            "\twhere lo_custkey = c_custkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5')\n"
            "\t\tand (s_city='UNITED KI1' or s_city='UNITED KI5')\n"
            "\t\tand d_year >= 1992 and d_year <= 1997\n"
            "\tgroup by c_city, s_city, d_year\n"
            "\torder by d_year asc, revenue desc;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, q10) {
    std::string query =
            "select c_city, s_city, d_year, sum(lo_revenue) as "
            "revenue\n"
            "\tfrom customer, lineorder, supplier, ddate\n"
            "\twhere lo_custkey = c_custkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5')\n"
            "\t\tand (s_city='UNITED KI1' or s_city='UNITED KI5')\n"
            "\t\tand d_yearmonth = 'Dec1997'\n"
            "\tgroup by c_city, s_city, d_year\n"
            "\torder by d_year asc, revenue desc;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 3);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, q11) {
    std::string query =
            "select d_year, c_nation, "
            "sum(lo_revenue) as profit1\n"
            "\tfrom ddate, customer, supplier, part, lineorder\n"
            "\twhere lo_partkey = p_partkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_custkey = c_custkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand c_region = 'AMERICA'\n"
            "\t\tand s_region = 'AMERICA'\n"
            "\t\tand (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')\n"
            "\tgroup by d_year, c_nation\n"
            "\torder by d_year, c_nation;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 4);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 2);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 2);

    EXPECT_EQ(select_resolver.project_references()->size(), 3);
}

TEST_F(SQLTest, q12) {
    std::string query =
            "select d_year, s_nation, p_category, sum(lo_revenue) "
            "as profit1\n"
            "\tfrom ddate, customer, supplier, part, lineorder\n"
            "\twhere lo_partkey = p_partkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_custkey = c_custkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand c_region = 'AMERICA'\n"
            "\t\tand s_region = 'AMERICA'\n"
            "\t\tand (d_year = 1997 or d_year = 1998)\n"
            "\t\tand (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')\n"
            "\tgroup by d_year, s_nation, p_category\n"
            "\torder by d_year, s_nation, p_category;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());

    select_resolver.ResolveSelectTree(queryTree);
    EXPECT_EQ(select_resolver.join_predicates().size(), 4);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 3);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, q13) {
    std::string query =
            "select d_year, s_city, p_brand1, sum(lo_revenue) as "
            "profit1\n"
            "\tfrom ddate, customer, supplier, part, lineorder\n"
            "\twhere lo_partkey = p_partkey\n"
            "\t\tand lo_suppkey = s_suppkey\n"
            "\t\tand lo_custkey = c_custkey\n"
            "\t\tand lo_orderdate = d_datekey\n"
            "\t\tand c_region = 'AMERICA'\n"
            "\t\tand s_nation = 'UNITED STATES'\n"
            "\t\tand (d_year = 1997 or d_year = 1998)\n"
            "\t\tand p_category = 'MFGR#14'\n"
            "\tgroup by d_year, s_city, p_brand1\n"
            "\torder by d_year, s_city, p_brand1;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());

    select_resolver.ResolveSelectTree(queryTree);
    EXPECT_EQ(select_resolver.join_predicates().size(), 4);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);

    EXPECT_EQ(select_resolver.groupby_references()->size(), 3);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 3);

    EXPECT_EQ(select_resolver.project_references()->size(), 4);
}

TEST_F(SQLTest, queryAggExpr) {
    std::string query =
            "select sum(lo_extendedprice*lo_discount) as "
            "revenue\n"
            "from lineorder, ddate\n"
            "where lo_orderdate = d_datekey\n"
            "and d_yearmonthnum = 199401\n"
            "and lo_discount < 6\n"
            "and lo_quantity < 35;";

    Sqlite3Select* queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);
    SelectResolver select_resolver(hustle_db->getCatalog());
    select_resolver.ResolveSelectTree(queryTree);

    EXPECT_EQ(select_resolver.join_predicates().size(), 1);

    EXPECT_EQ(select_resolver.agg_references()->size(), 1);
    EXPECT_TRUE((*select_resolver.agg_references())[0].expr_ref != nullptr);
    EXPECT_EQ((*select_resolver.agg_references())[0]
                      .expr_ref->left_expr->column_ref->col_name,
              "lo_extendedprice");
    EXPECT_EQ((*select_resolver.agg_references())[0]
                      .expr_ref->right_expr->column_ref->col_name,
              "lo_discount");
    EXPECT_EQ((*select_resolver.agg_references())[0]
                      .expr_ref->op, TK_STAR);
    EXPECT_EQ(select_resolver.groupby_references()->size(), 0);
    EXPECT_EQ(select_resolver.orderby_references()->size(), 0);

    EXPECT_EQ(select_resolver.project_references()->size(), 1);

    query =
            "select sum(lo_extendedprice) as "
            "revenue\n"
            "from lineorder, ddate\n"
            "where lo_orderdate = d_datekey\n"
            "and d_yearmonthnum = 199401\n"
            "and lo_discount < 6\n"
            "and lo_quantity < 35;";
    queryTree = (Sqlite3Select*)hustle::utils::executeSqliteParse(
            hustle_db->getSqliteDBPath(), query);

    SelectResolver select_resolver2(hustle_db->getCatalog());
    select_resolver2.ResolveSelectTree(queryTree);
    EXPECT_TRUE((*select_resolver2.agg_references())[0].expr_ref == nullptr);
}

