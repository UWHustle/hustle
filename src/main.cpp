#include <iostream>

#include "sqlite3.h"
//#include "frontend/HustleFrontend.h"
#include "catalog/Catalog.h"

const std::string kDBPath = "";

namespace {
void createTestDB(hustle::catalog::Catalog* catalog) {
  hustle::catalog::TableSchema ts("Subscriber");
  hustle::catalog::ColumnSchema c1("c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c2("c2", {hustle::catalog::HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  catalog->addTable(ts);

  hustle::catalog::TableSchema ts1("AccessInfo");
  hustle::catalog::ColumnSchema c3("c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema c4("c4", {hustle::catalog::HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});
  catalog->addTable(ts1);
}
}

int main(int argc, char *argv[]) {

//  hustle::catalog::Catalog catalog(kDBPath);

//  createTestDB(&catalog);



//  hustle::frontend::HustleFrontend hustle_frontend;
//
//  std::string query = "EXPLAIN QUERY PLAN select R.B from R,S where R.A=S.A;";
//  std::cout << "For query: " << query << std::endl;
//  std::cout << "The produced plan is: " << hustle_frontend.SqlToPlan(query)
//            << std::endl;


  if ( true != false ) {std::cout << "1" << std::endl;}
  if ( false != true ) {std::cout << "2" << std::endl;}
  if ( true != true ) {std::cout << "3" << std::endl;}
  if ( false != false ) {std::cout << "4" << std::endl;}
  return 0;
}