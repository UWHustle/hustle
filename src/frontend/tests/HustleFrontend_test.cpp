#include <stdio.h>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"

#include "api/HustleDB.h"
#include "catalog/Catalog.h"

#include "frontend/ParseTree.h"

using namespace testing;
using nlohmann::json;

char project[1024];
char loopPred[1024];
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
  memset(loopPred, 0, 1024);
  memset(otherPred, 0, 1024);

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  std::string text = "{\"project\": [" + std::string(project) + "], \"loop_pred\": [" + std::string(loopPred) + "], \"other_pred\": [" + std::string(otherPred) + "]}";

  json j = json::parse(text);
  hustle::frontend::ParseTree my_parse_tree = j;
  auto out = j.dump(4);
  std::cout << out << std::endl;

  /// build validation parse tree
  auto c00 = std::make_shared<hustle::frontend::Column>(0, 0);
  auto c10 = std::make_shared<hustle::frontend::Column>(1, 0);

  hustle::frontend::LoopPredicate loop_predicate_0(0, std::vector<hustle::frontend::Predicate>{});
  hustle::frontend::Predicate pred = hustle::frontend::Predicate(c10, 53, c00);
  hustle::frontend::LoopPredicate loop_predicate_1(1, std::vector<hustle::frontend::Predicate>{std::move(pred)});

  hustle::frontend::ParseTree parse_tree_val = hustle::frontend::ParseTree(
      std::vector<std::string>({"Subscriber.c1"}),
      std::vector<hustle::frontend::LoopPredicate>({std::move(loop_predicate_0), std::move(loop_predicate_1)}),
      std::vector<hustle::frontend::Predicate>{});

  json j_val = parse_tree_val;
  auto out_val = j_val.dump(4);

  EXPECT_EQ(out, out_val);
}

TEST(Frontend, test2) {
  if (!std::filesystem::exists("db_directory")) {
    createTable();
  }

  hustle::HustleDB hustleDB("db_directory");

  memset(project, 0, 1024);
  memset(loopPred, 0, 1024);
  memset(otherPred, 0, 1024);

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and AccessInfo.c4 < 5;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  std::string text = "{\"project\": [" + std::string(project) + "], \"loop_pred\": [" + std::string(loopPred) + "], \"other_pred\": [" + std::string(otherPred) + "]}";

  json j = json::parse(text);
  hustle::frontend::ParseTree my_parse_tree = j;
  auto out = j.dump(4);
  std::cout << out << std::endl;

  /// build validation parse tree
  auto c00 = std::make_shared<hustle::frontend::Column>(0, 0);
  auto c01 = std::make_shared<hustle::frontend::Column>(0, 1);
  auto c10 = std::make_shared<hustle::frontend::Column>(1, 0);
  auto c11 = std::make_shared<hustle::frontend::Column>(1, 1);
  auto i2 = std::make_shared<hustle::frontend::Integer>(2);
  auto i5 = std::make_shared<hustle::frontend::Integer>(5);

  hustle::frontend::Predicate pred = hustle::frontend::Predicate(c01, 54, i2);
  hustle::frontend::LoopPredicate loop_predicate_0(0, std::vector<hustle::frontend::Predicate>{std::move(pred)});
  pred = hustle::frontend::Predicate(c10, 53, c00);
  hustle::frontend::LoopPredicate loop_predicate_1(1, std::vector<hustle::frontend::Predicate>{std::move(pred)});
  hustle::frontend::Predicate other_pred = hustle::frontend::Predicate(std::move(c11), 56, std::move(i5));

  hustle::frontend::ParseTree parse_tree_val = hustle::frontend::ParseTree(
      std::vector<std::string>({"Subscriber.c1"}),
      std::vector<hustle::frontend::LoopPredicate>({std::move(loop_predicate_0), std::move(loop_predicate_1)}),
      std::vector<hustle::frontend::Predicate>{std::move(other_pred)});


  json j_val = parse_tree_val;
  auto out_val = j_val.dump(4);

  EXPECT_EQ(out, out_val);

}

