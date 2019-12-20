#include <iostream>

#include "sqlite3.h"
#include "frontend/HustleFrontend.h"
#include "catalog/Catalog.h"

int main(int argc, char *argv[]) {
  
  hustle::catalog::Catalog catalog;
  
  std::cout << "Hello World " << std::endl;

  hustle::frontend::HustleFrontend hustle_frontend;

  std::string query = "EXPLAIN QUERY PLAN select R.B from R,S where R.A=S.A;";
  std::cout << "For query: " << query << std::endl;
  std::cout << "The produced plan is: " << hustle_frontend.SqlToPlan(query)
            << std::endl;

  return 0;
}