#include <iostream>

#include "api/HustleDB.h"
#include "catalog/Catalog.h"
#include "catalog/TableSchema.h"
#include "catalog/ColumnSchema.h"
#include <parser/ParseTree.h>
#include <resolver/Resolver.h>


extern const int SERIAL_BLOCK_SIZE = 4096;
char project[SERIAL_BLOCK_SIZE];
char loopPred[SERIAL_BLOCK_SIZE];
char otherPred[SERIAL_BLOCK_SIZE];
char groupBy[SERIAL_BLOCK_SIZE];
char orderBy[SERIAL_BLOCK_SIZE];
char* currPos = nullptr;

int main(int argc, char *argv[]) {
  std::filesystem::remove_all("db_directory");
  hustle::HustleDB hustleDB("db_directory");

  // Create table Subscriber
  hustle::catalog::TableSchema ts("Subscriber");
  hustle::catalog::ColumnSchema c1("c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c2("c2", {hustle::catalog::HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({});

  hustleDB.createTable(ts);

  // Create table rAccessInfo
  hustle::catalog::TableSchema ts1("AccessInfo");
  hustle::catalog::ColumnSchema c3("c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({});

  hustleDB.createTable(ts1);

  // Get Execution Plan
  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and AccessInfo.c4 < 5;";

  std::cout << "For query: " << query << std::endl <<
                "The plan is: " << std::endl <<
                hustleDB.getPlan(query) << std::endl;

  std::string text =
      "{\"project\": [" + std::string(project) + "], \"loop_pred\": [" + std::string(loopPred) + "], \"other_pred\": ["
          + std::string(otherPred) + "], \"group_by\": [" + std::string(groupBy) + "], \"order_by\": [" + std::string(orderBy) + "]}";

  nlohmann::json j = nlohmann::json::parse(text);
  std::shared_ptr<hustle::parser::ParseTree> parse_tree = std::make_shared<hustle::parser::ParseTree>(j);
  std::cout << j.dump(4) << std::endl;

  auto resolver = std::make_shared<hustle::resolver::Resolver>();

  auto plan = resolver->resolve(parse_tree);

  return 0;
}
