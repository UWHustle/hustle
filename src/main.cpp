#include <iostream>

#include "api/HustleDB.h"
#include "catalog/Catalog.h"
#include "catalog/TableSchema.h"
#include "catalog/ColumnSchema.h"

char project[1024];
char join[1024];
char nonJoin[1024];

int main(int argc, char *argv[]) {

  hustle::HustleDB hustleDB("db_directory");

  // Create table Subscriber
  hustle::catalog::TableSchema ts("Subscriber");
  hustle::catalog::ColumnSchema c1("c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c2("c2", {hustle::catalog::HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});

  hustleDB.createTable(ts);

  // Create table AccessInfo
  hustle::catalog::TableSchema ts1("AccessInfo");
  hustle::catalog::ColumnSchema c3("c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});

  hustleDB.createTable(ts1);

  memset(project, 0, 1024);
  memset(join, 0, 1024);
  memset(nonJoin, 0, 1024);

  // Get Execution Plan
  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c2 = AccessInfo.c3;";

  std::cout << "For query: " << query << std::endl <<
                "The plan is: " << std::endl <<
                hustleDB.getPlan(query) << std::endl;

  fprintf(stdout, R"({"project": [%s], "join": [%s], "non-join": [%s]})", project, join, nonJoin);

  return 0;
}
