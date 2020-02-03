#include <stdio.h>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"

#include "api/HustleDB.h"
#include "catalog/Catalog.h"

#include "frontend/ParseTree.h"

using namespace testing;

char project[1024];
char indexPred[1024];
char otherPred[1024];

void createTable() {
  std::filesystem::remove_all("db_directory");
  EXPECT_FALSE(std::filesystem::exists("db_directory"));

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
}

TEST(Frontend, test1) {
  if (!std::filesystem::exists("db_directory")) {
    createTable();
  }

  hustle::HustleDB hustleDB("db_directory");

  memset(project, 0, 1024);
  memset(indexPred, 0, 1024);
  memset(otherPred, 0, 1024);

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  fprintf(stdout, R"({"project": [%s], "index-pred": [%s], "other-pred": [%s]})""\n", project, indexPred, otherPred);
  EXPECT_STREQ(project, R"("Subscriber.c1")");
  EXPECT_STREQ(indexPred, R"({"fromtable": 0, "predicates": []}, {"fromtable": 1, "predicates": [{"left": {"iTable": 1, "iColumn": 0}, "op": 53, "right": {"iTable": 0, "iColumn": 0}}]})");
  EXPECT_STREQ(otherPred, R"()");

}

TEST(Frontend, test2) {
  if (!std::filesystem::exists("db_directory")) {
    createTable();
  }

  hustle::HustleDB hustleDB("db_directory");

  memset(project, 0, 1024);
  memset(indexPred, 0, 1024);
  memset(otherPred, 0, 1024);

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and AccessInfo.c4 < 5;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  fprintf(stdout, R"({"project": [%s], "index-pred": [%s], "other-pred": [%s]})""\n", project, indexPred, otherPred);
  EXPECT_STREQ(project, R"("Subscriber.c1")");
  EXPECT_STREQ(indexPred, R"({"fromtable": 0, "predicates": [{"left": {"iTable": 0, "iColumn": 1}, "op": 54, "right": {"value": 2}}]}, {"fromtable": 1, "predicates": [{"left": {"iTable": 1, "iColumn": 0}, "op": 53, "right": {"iTable": 0, "iColumn": 0}}]})");
  EXPECT_STREQ(otherPred, R"({"left": {"iTable": 1, "iColumn": 1}, "op": 56, "right": {"value": 5}})");

}


