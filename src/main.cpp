#include <iostream>

#include "api/HustleDB.h"
#include "catalog/Catalog.h"
#include "catalog/TableSchema.h"
#include "catalog/ColumnSchema.h"
#include <frontend/ParseTree.h>

using hustle::frontend::ParseTree;

char project[1024];
char loopPred[4096];
char otherPred[4096];
char groupBy[4096];
char orderBy[4096];
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
  ts.setPrimaryKey({"c1", "c2"});

  hustleDB.createTable(ts);

  // Create table rAccessInfo
  hustle::catalog::TableSchema ts1("AccessInfo");
  hustle::catalog::ColumnSchema c3("c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});

  hustleDB.createTable(ts1);

  memset(project, 0, 1024);
  memset(loopPred, 0, 1024);
  memset(otherPred, 0, 1024);

  // Get Execution Plan
  std::string query = "EXPLAIN QUERY PLAN select Subscriber.c1 "
                      "from Subscriber, AccessInfo "
                      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c1 > 2;";

  std::cout << "For query: " << query << std::endl <<
                "The plan is: " << std::endl <<
                hustleDB.getPlan(query) << std::endl;


  std::string plan_path = "db_directory/plan.json";
  FILE* fp = fopen(plan_path.c_str(), "w");
  fprintf(fp, R"({"execution_plan": {"project": [%s], "loop_pred": [%s], "other_pred": [%s]}})", project, loopPred, otherPred);
  fprintf(stdout, R"({"execution_plan": {"project": [%s], "loop_pred": [%s], "other_pred": [%s]}})", project, loopPred, otherPred);
  fclose(fp);

  ParseTree parseTree;

  // std::ifstream in(plan_path);
  // {
  //   cereal::JSONInputArchive iarchive(in);
  //   iarchive(parseTree);
  // }
  //
  // cereal::JSONOutputArchive output(std::cout);
  //
  // output( cereal::make_nvp("execution_plan", parseTree) );

  return 0;
}
