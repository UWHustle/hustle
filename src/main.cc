#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "catalog/column_schema.h"
#include "catalog/table_schema.h"
#include "parser/parser.h"
#include "storage/util.h"

int main(int argc, char *argv[]) {
  std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema,
      d_schema;

  // Create table part
  hustle::catalog::TableSchema part("part");
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
  p_schema = part.getArrowSchema();

  // Create table supplier
  hustle::catalog::TableSchema supplier("supplier");
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
  supplier.addColumn(s_suppkey);
  supplier.addColumn(s_name);
  supplier.addColumn(s_address);
  supplier.addColumn(s_city);
  supplier.addColumn(s_nation);
  supplier.addColumn(s_region);
  supplier.addColumn(s_phone);
  supplier.setPrimaryKey({});

  s_schema = supplier.getArrowSchema();

  // Create table customer
  hustle::catalog::TableSchema customer("customer");
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
  c_schema = customer.getArrowSchema();

  // Create table ddate
  hustle::catalog::TableSchema ddate("ddate");
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
      "d_yearmonthnum", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema d_yearmonth(
      "d_yearmonth", {hustle::catalog::HustleType::CHAR, 8}, true, false);
  hustle::catalog::ColumnSchema d_daynuminweek(
      "d_daynuminweek", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema d_daynuminmonth(
      "d_daynuminmonth", {hustle::catalog::HustleType::INTEGER, 0}, true,
      false);
  hustle::catalog::ColumnSchema d_daynuminyear(
      "d_daynuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema d_monthnuminyear(
      "d_monthnuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
      false);
  hustle::catalog::ColumnSchema d_weeknuminyear(
      "d_weeknuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
      false);
  hustle::catalog::ColumnSchema d_sellingseason(
      "d_sellingseason", {hustle::catalog::HustleType::CHAR, 13}, true, false);
  hustle::catalog::ColumnSchema d_lastdayinweekfl(
      "d_lastdayinweekfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
  hustle::catalog::ColumnSchema d_lastdayinmonthfl(
      "d_lastdayinmonthfl", {hustle::catalog::HustleType::CHAR, 1}, true,
      false);
  hustle::catalog::ColumnSchema d_holidayfl(
      "d_holidayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
  hustle::catalog::ColumnSchema d_weekdayfl(
      "d_weekdayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
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
  d_schema = ddate.getArrowSchema();

  // Create table lineorder
  hustle::catalog::TableSchema lineorder("lineorder");
  hustle::catalog::ColumnSchema lo_orderkey(
      "lo_orderkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_linenumber(
      "lo_linenumber", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_custkey(
      "lo_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_partkey(
      "lo_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_suppkey(
      "lo_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_orderdate(
      "lo_orderdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_orderpriority(
      "lo_orderpriority", {hustle::catalog::HustleType::CHAR, 15}, true, false);
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
      "lo_supplycost", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_tax(
      "lo_tax", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_commitdate(
      "lo_commitdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema lo_shipmode(
      "lo_shipmode", {hustle::catalog::HustleType::CHAR, 10}, true, false);
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
  lo_schema = lineorder.getArrowSchema();

  std::shared_ptr<DBTable> t;

  std::cout << "read the table files..." << std::endl;
  std::shared_ptr<DBTable> lo, c, s, p, d;
  lo = read_from_file("../ssb/data/lineorder.hsl");
  d = read_from_file("../ssb/data/date.hsl");
  p = read_from_file("../ssb/data/part.hsl");
  c = read_from_file("../ssb/data/customer.hsl");
  s = read_from_file("../ssb/data/supplier.hsl");

  std::filesystem::remove_all("db_directory");
  // EXPECT_FALSE(std::filesystem::exists("db_directory"));

  hustle::HustleDB hustleDB("db_directory");

  hustleDB.createTable(part, p);

  hustleDB.createTable(supplier, s);

  hustleDB.createTable(customer, c);

  hustleDB.createTable(ddate, d);

  hustleDB.createTable(lineorder, lo);

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

  hustleDB.executeQuery(query);

  return 0;
}
