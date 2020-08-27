#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "catalog/column_schema.h"
#include "catalog/table_schema.h"
#include "parser/parser.h"
#include "resolver/resolver.h"

int main(int argc, char *argv[]) {
  std::filesystem::remove_all("db_directory");
  hustle::HustleDB hustleDB("db_directory");

  // Create table Subscriber
  hustle::catalog::TableSchema ts("Subscriber");
  hustle::catalog::ColumnSchema c1(
      "c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c2(
      "c2", {hustle::catalog::HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({});

  hustleDB.createTable(ts);

  // Create table rAccessInfo
  hustle::catalog::TableSchema ts1("AccessInfo");
  hustle::catalog::ColumnSchema c3(
      "c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5},
                                   false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({});

  hustleDB.createTable(ts1);

  // Get Execution Plan
  std::string query =
      "EXPLAIN QUERY PLAN select Subscriber.c1 "
      "from Subscriber, AccessInfo "
      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and "
      "AccessInfo.c4 < 5;";

  auto parser = std::make_shared<hustle::parser::Parser>();
  auto resolver =
      std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
  parser->parse(query, hustleDB);
  resolver->resolve(parser->getParseTree());
  std::cout << resolver->toString(4) << std::endl;

  return 0;
}
