#include <iostream>
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"

#include "api/HustleDB.h"
#include "catalog/Catalog.h"

#include "parser/ParseTree.h"

using namespace testing;
using namespace hustle::parser;
using nlohmann::json;

extern const int SERIAL_BLOCK_SIZE = 4096;
char project[SERIAL_BLOCK_SIZE];
char loopPred[SERIAL_BLOCK_SIZE];
char otherPred[SERIAL_BLOCK_SIZE];
char groupBy[SERIAL_BLOCK_SIZE];
char orderBy[SERIAL_BLOCK_SIZE];
char* currPos = nullptr;

class ParserSimpleTest : public Test {
  void SetUp() override {
    /**
      CREATE TABLE Subscriber(c1 INT  NOT NULL , c2 CHAR(10)  UNIQUE);
      CREATE TABLE AccessInfo(c3 INT  NOT NULL , c4 CHAR(5)  UNIQUE);
     */

    std::filesystem::remove_all("db_directory");
    EXPECT_FALSE(std::filesystem::exists("db_directory"));

    hustle::HustleDB hustleDB("db_directory");

    // Create table Subscriber
    hustle::catalog::TableSchema ts("Subscriber");
    hustle::catalog::ColumnSchema c1("c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema c2("c2", {hustle::catalog::HustleType::CHAR, 10}, false, true);
    ts.addColumn(c1);
    ts.addColumn(c2);
    // ts.setPrimaryKey({"c1", "c2"});
    ts.setPrimaryKey({});

    hustleDB.createTable(ts);

    // Create table AccessInfo
    hustle::catalog::TableSchema ts1("AccessInfo");
    hustle::catalog::ColumnSchema c3("c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5}, false, true);
    ts1.addColumn(c3);
    ts1.addColumn(c4);
    // ts1.setPrimaryKey({"c3"});
    ts1.setPrimaryKey({});

    hustleDB.createTable(ts1);
  }
};


TEST_F(ParserSimpleTest, test1) {
  hustle::HustleDB hustleDB("db_directory");

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  std::string text =
      "{\"project\": [" + std::string(project) + "], \"loop_pred\": [" + std::string(loopPred) + "], \"other_pred\": ["
          + std::string(otherPred) + "], \"group_by\": [" + std::string(groupBy) + "], \"order_by\": [" + std::string(orderBy) + "]}";

  json j = json::parse(text);
  ParseTree my_parse_tree = j;
  auto out = j.dump(4);
  // std::cout << out << std::endl;

  /// build validation parse tree
  auto c00 = std::make_shared<Column>("c1", 0, 0);
  auto c10 = std::make_shared<Column>("c3", 1, 0);

  std::shared_ptr<LoopPredicate> loop_predicate_0 = std::make_shared<LoopPredicate>(0, std::vector<std::shared_ptr<Expr>>{});
  std::shared_ptr<Expr> pred = std::make_shared<CompositeExpr>(c10, 53, c00);
  std::shared_ptr<LoopPredicate> loop_predicate_1 = std::make_shared<LoopPredicate>(1, std::vector<std::shared_ptr<Expr>>{std::move(pred)});
  std::shared_ptr<Project> proj_0 = std::make_shared<Project>("Subscriber.c1", c00);

  ParseTree parse_tree_val = ParseTree(
      std::vector<std::shared_ptr<Project>>({proj_0}),
      std::vector<std::shared_ptr<LoopPredicate>>({std::move(loop_predicate_0), std::move(loop_predicate_1)}),
      std::vector<std::shared_ptr<Expr>>{},
      std::vector<std::shared_ptr<Expr>>{},
      std::vector<std::shared_ptr<OrderBy>>{});

  json j_val = parse_tree_val;
  auto out_val = j_val.dump(4);

  EXPECT_EQ(out, out_val);
}

TEST_F(ParserSimpleTest, test2) {
  hustle::HustleDB hustleDB("db_directory");

  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and AccessInfo.c4 < 5;";

  std::cout << "For query: " << query << std::endl <<
            "The plan is: " << std::endl <<
            hustleDB.getPlan(query) << std::endl;

  std::string text =
      "{\"project\": [" + std::string(project) + "], \"loop_pred\": [" + std::string(loopPred) + "], \"other_pred\": ["
          + std::string(otherPred) + "], \"group_by\": [" + std::string(groupBy) + "], \"order_by\": [" + std::string(orderBy) + "]}";

  json j = json::parse(text);
  ParseTree my_parse_tree = j;
  auto out = j.dump(4);
  std::cout << out << std::endl;

  /// build validation parse tree
  auto c00 = std::make_shared<Column>("c1", 0, 0);
  auto c01 = std::make_shared<Column>("c2", 0, 1);
  auto c10 = std::make_shared<Column>("c3", 1, 0);
  auto c11 = std::make_shared<Column>("c4", 1, 1);
  auto i2 = std::make_shared<IntLiteral>(2);
  auto i5 = std::make_shared<IntLiteral>(5);

  std::shared_ptr<Expr> pred = std::make_shared<CompositeExpr>(c01, 54, i2);
  std::shared_ptr<LoopPredicate> loop_predicate_0 = std::make_shared<LoopPredicate>(0, std::vector<std::shared_ptr<Expr>>{std::move(pred)});
  pred = std::make_shared<CompositeExpr>(c10, 53, c00);
  std::shared_ptr<LoopPredicate> loop_predicate_1 = std::make_shared<LoopPredicate>(1, std::vector<std::shared_ptr<Expr>>{std::move(pred)});
  std::shared_ptr<Expr> other_pred = std::make_shared<CompositeExpr>(std::move(c11), 56, std::move(i5));
  std::shared_ptr<Project> proj_0 = std::make_shared<Project>("Subscriber.c1", c00);


  ParseTree parse_tree_val = ParseTree(
      std::vector<std::shared_ptr<Project>>({proj_0}),
      std::vector<std::shared_ptr<LoopPredicate>>({std::move(loop_predicate_0), std::move(loop_predicate_1)}),
      std::vector<std::shared_ptr<Expr>>{std::move(other_pred)},
      std::vector<std::shared_ptr<Expr>>{},
      std::vector<std::shared_ptr<OrderBy>>{});


  json j_val = parse_tree_val;
  auto out_val = j_val.dump(4);

  EXPECT_EQ(out, out_val);

}




