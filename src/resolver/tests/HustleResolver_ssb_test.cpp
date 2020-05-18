#include <iostream>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"

#include "api/HustleDB.h"
#include "catalog/Catalog.h"
#include "parser/Parser.h"
#include <resolver/Resolver.h>

using namespace testing;
using namespace hustle::parser;
using nlohmann::json;

class ResolverSSBTest : public Test {
protected:
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

        hustle::HustleDB hustleDB("db_directory");

        // Create table part
        hustle::catalog::TableSchema part("part");
        hustle::catalog::ColumnSchema p_partkey("p_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_name("p_name", {hustle::catalog::HustleType::CHAR, 22}, true, false);
        hustle::catalog::ColumnSchema p_mfgr("p_mfgr", {hustle::catalog::HustleType::CHAR, 6}, true, false);
        hustle::catalog::ColumnSchema p_category("p_category", {hustle::catalog::HustleType::CHAR, 7}, true, false);
        hustle::catalog::ColumnSchema p_brand1("p_brand1", {hustle::catalog::HustleType::CHAR, 9}, true, false);
        hustle::catalog::ColumnSchema p_color("p_color", {hustle::catalog::HustleType::CHAR, 11}, true, false);
        hustle::catalog::ColumnSchema p_type("p_type", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema p_size("p_size", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_container("p_container", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        part.addColumn(p_partkey);
        part.addColumn(p_name);
        part.addColumn(p_mfgr);
        part.addColumn(p_category);
        part.addColumn(p_brand1);
        part.addColumn(p_color);
        part.addColumn(p_type);
        part.addColumn(p_size);
        part.addColumn(p_container);
        part.setPrimaryKey({});

        hustleDB.createTable(part);

        // Create table supplier
        hustle::catalog::TableSchema supplier("supplier");
        hustle::catalog::ColumnSchema s_suppkey("s_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema s_name("s_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_address("s_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_city("s_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema s_nation("s_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema s_region("s_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
        hustle::catalog::ColumnSchema s_phone("s_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        supplier.addColumn(s_suppkey);
        supplier.addColumn(s_name);
        supplier.addColumn(s_address);
        supplier.addColumn(s_city);
        supplier.addColumn(s_nation);
        supplier.addColumn(s_region);
        supplier.addColumn(s_phone);
        supplier.setPrimaryKey({});

        hustleDB.createTable(supplier);

        // Create table customer
        hustle::catalog::TableSchema customer("customer");
        hustle::catalog::ColumnSchema c_suppkey("c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema c_name("c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema c_address("c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema c_city("c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema c_nation("c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema c_region("c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
        hustle::catalog::ColumnSchema c_phone("c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema c_mktsegment("c_mktsegment", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        customer.addColumn(c_suppkey);
        customer.addColumn(c_name);
        customer.addColumn(c_address);
        customer.addColumn(c_city);
        customer.addColumn(c_nation);
        customer.addColumn(c_region);
        customer.addColumn(c_phone);
        customer.addColumn(c_mktsegment);
        customer.setPrimaryKey({});

        hustleDB.createTable(customer);

        // Create table ddate
        hustle::catalog::TableSchema ddate("ddate");
        hustle::catalog::ColumnSchema d_datekey("d_datekey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_date("d_date", {hustle::catalog::HustleType::CHAR, 19}, true, false);
        hustle::catalog::ColumnSchema d_dayofweek("d_dayofweek", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_month("d_month", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_year("d_year", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_yearmonthnum("d_yearmonthnum", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_yearmonth("d_yearmonth", {hustle::catalog::HustleType::CHAR, 8}, true, false);
        hustle::catalog::ColumnSchema d_daynuminweek("d_daynuminweek", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_daynuminmonth("d_daynuminmonth", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_daynuminyear("d_daynuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_monthnuminyear("d_monthnuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_weeknuminyear("d_weeknuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_sellingseason("d_sellingseason", {hustle::catalog::HustleType::CHAR, 13}, true, false);
        hustle::catalog::ColumnSchema d_lastdayinweekfl("d_lastdayinweekfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_lastdayinmonthfl("d_lastdayinmonthfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_holidayfl("d_holidayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_weekdayfl("d_weekdayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        ddate.addColumn(d_datekey);
        ddate.addColumn(d_date);
        ddate.addColumn(d_dayofweek);
        ddate.addColumn(d_month);
        ddate.addColumn(d_year);
        ddate.addColumn(d_yearmonthnum);
        ddate.addColumn(d_yearmonth);
        ddate.addColumn(d_daynuminweek);
        ddate.addColumn(d_daynuminmonth);
        ddate.addColumn(d_daynuminyear);
        ddate.addColumn(d_monthnuminyear);
        ddate.addColumn(d_weeknuminyear);
        ddate.addColumn(d_sellingseason);
        ddate.addColumn(d_lastdayinweekfl);
        ddate.addColumn(d_lastdayinmonthfl);
        ddate.addColumn(d_holidayfl);
        ddate.addColumn(d_weekdayfl);
        ddate.setPrimaryKey({});

        hustleDB.createTable(ddate);

        // Create table lineorder
        hustle::catalog::TableSchema lineorder("lineorder");
        hustle::catalog::ColumnSchema lo_orderkey("lo_orderkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_linenumber("lo_linenumber", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_custkey("lo_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_partkey("lo_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_suppkey("lo_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_orderdate("lo_orderdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_orderpriority("lo_orderpriority", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema lo_shippriority("lo_shippriority", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema lo_quantity("lo_quantity", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_extendedprice("lo_extendedprice", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_ordertotalprice("lo_ordertotalprice", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_discount("lo_discount", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_revenue("lo_revenue", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_supplycost("lo_supplycost", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_tax("lo_tax", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_commitdate("lo_commitdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_shipmode("lo_shipmode", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        lineorder.addColumn(lo_orderkey);
        lineorder.addColumn(lo_linenumber);
        lineorder.addColumn(lo_custkey);
        lineorder.addColumn(lo_partkey);
        lineorder.addColumn(lo_suppkey);
        lineorder.addColumn(lo_orderdate);
        lineorder.addColumn(lo_orderpriority);
        lineorder.addColumn(lo_shippriority);
        lineorder.addColumn(lo_quantity);
        lineorder.addColumn(lo_extendedprice);
        lineorder.addColumn(lo_ordertotalprice);
        lineorder.addColumn(lo_discount);
        lineorder.addColumn(lo_revenue);
        lineorder.addColumn(lo_supplycost);
        lineorder.addColumn(lo_tax);
        lineorder.addColumn(lo_commitdate);
        lineorder.addColumn(lo_shipmode);
        lineorder.setPrimaryKey({});

        hustleDB.createTable(lineorder);
    }
};

TEST_F(ResolverSSBTest, ssb_q1) {
    std::cout << "Debug 1 " << std::endl;
    hustle::HustleDB hustleDB("db_directory");
  std::cout << "Debug 2 " << std::endl;
    std::string query = "EXPLAIN QUERY PLAN select sum(lo_extendedprice * lo_discount) as revenue "
                        "from lineorder, ddate "
                        "where lo_orderdate = d_datekey and d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;";
  std::cout << "Debug 3 " << std::endl;
    hustleDB.getPlan(query);
  std::cout << "Debug 4 " << std::endl;
    auto parser = std::make_shared<hustle::parser::Parser>();
  std::cout << "Debug 5 " << std::endl;
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
  std::cout << "Debug 6 " << std::endl;
    parser->parse(query, hustleDB);
  std::cout << "Debug7 " << std::endl;
    resolver->resolve(parser->getParseTree());
  std::cout << "Debug 8 " << std::endl;
  std::cout << "Plan:" << resolver->toString(4) << std::endl;
    // TODO(Lichengxi): build validation plan

}

TEST_F(ResolverSSBTest, ssb_q2) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select sum(lo_extendedprice * lo_discount) as revenue\n"
                        "from lineorder, ddate\n"
                        "where lo_orderdate = d_datekey\n"
                        "and d_yearmonthnum = 199401\n"
                        "and lo_discount between 4 and 6\n"
                        "and lo_quantity between 26 and 35;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;
    // TODO(Lichengxi): build validation plan

}

TEST_F(ResolverSSBTest, ssb_q3) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select sum(lo_extendedprice * lo_discount) as revenue\n"
                        "from lineorder, ddate\n"
                        "where lo_orderdate = d_datekey\n"
                        "and d_weeknuminyear = 6 and d_year = 1994\n"
                        "and lo_discount between 5 and 7\n"
                        "and lo_quantity between 36 and 40;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
//     std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q4) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select sum(lo_revenue), d_year, p_brand1\n"
                        "from lineorder, ddate, part, supplier\n"
                        "where lo_partkey = p_partkey\n"
                        "and lo_suppkey = s_suppkey\n"
                        "and lo_orderdate = d_datekey\n"
                        "and p_category = 'MFGR#12'\n"
                        "and s_region = 'AMERICA'\n"
                        "group by d_year, p_brand1\n"
                        "order by d_year, p_brand1;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
   //  std::cout << "Plan:" << parser->toString(4) << std::endl;
    resolver->resolve(parser->getParseTree());
//     std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q5) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select sum(lo_revenue), d_year, p_brand1\n"
                        "\tfrom lineorder, ddate, part, supplier\n"
                        "\twhere lo_partkey = p_partkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand p_brand1 between 'MFGR#2221' and 'MFGR#2228'\n"
                        "\t\tand s_region = 'ASIA'\n"
                        "\tgroup by d_year, p_brand1\n"
                        "\torder by d_year, p_brand1;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q6) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select sum(lo_revenue), d_year, p_brand1\n"
                        "\tfrom lineorder, ddate, part, supplier\n"
                        "\twhere lo_partkey = p_partkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand p_brand1 = 'MFGR#2221'\n"
                        "\t\tand s_region = 'EUROPE'\n"
                        "\tgroup by d_year, p_brand1\n"
                        "\torder by d_year, p_brand1;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q7) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select c_nation, s_nation, d_year, sum(lo_revenue) as revenue\n"
                        "\tfrom customer, lineorder, supplier, ddate\n"
                        "\twhere lo_custkey = c_custkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand c_region = 'ASIA'\n"
                        "\t\tand s_region = 'ASIA'\n"
                        "\t\tand d_year >= 1992 and d_year <= 1997\n"
                        "\tgroup by c_nation, s_nation, d_year\n"
                        "\torder by d_year asc, revenue desc;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
   //  std::cout << "Plan:" << parser->toString(4) << std::endl;
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q8) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
                        "\tfrom customer, lineorder, supplier, ddate\n"
                        "\twhere lo_custkey = c_custkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand c_nation = 'UNITED STATES'\n"
                        "\t\tand s_nation = 'UNITED STATES'\n"
                        "\t\tand d_year >= 1992 and d_year <= 1997\n"
                        "\tgroup by c_city, s_city, d_year\n"
                        "\torder by d_year asc, revenue desc;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q9) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
                        "\tfrom customer, lineorder, supplier, ddate\n"
                        "\twhere lo_custkey = c_custkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5')\n"
                        "\t\tand (s_city='UNITED KI1' or s_city='UNITED KI5')\n"
                        "\t\tand d_year >= 1992 and d_year <= 1997\n"
                        "\tgroup by c_city, s_city, d_year\n"
                        "\torder by d_year asc, revenue desc;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q10) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select c_city, s_city, d_year, sum(lo_revenue) as revenue\n"
                        "\tfrom customer, lineorder, supplier, ddate\n"
                        "\twhere lo_custkey = c_custkey\n"
                        "\t\tand lo_suppkey = s_suppkey\n"
                        "\t\tand lo_orderdate = d_datekey\n"
                        "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5')\n"
                        "\t\tand (s_city='UNITED KI1' or s_city='UNITED KI5')\n"
                        "\t\tand d_yearmonth = 'Dec1997'\n"
                        "\tgroup by c_city, s_city, d_year\n"
                        "\torder by d_year asc, revenue desc;";

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q11) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select d_year, c_nation, sum(lo_revenue-lo_supplycost) as profit1\n"
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

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q12) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select d_year, s_nation, p_category, sum(lo_revenue) as profit1\n"
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

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSSBTest, ssb_q13) {
    hustle::HustleDB hustleDB("db_directory");

    std::string query = "EXPLAIN QUERY PLAN select d_year, s_city, p_brand1, sum(lo_revenue) as profit1\n"
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

    hustleDB.getPlan(query);

    auto parser = std::make_shared<hustle::parser::Parser>();
    auto resolver = std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
    parser->parse(query, hustleDB);
    resolver->resolve(parser->getParseTree());
   //  std::cout << "Plan:" << resolver->toString(4) << std::endl;

    // TODO(Lichengxi): build validation plan

}