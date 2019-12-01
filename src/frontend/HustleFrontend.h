/* Parses a sql statement and produces an execution plan.
 * SqLite3 is used to parse the sql string and using its explain
 * output we get a plan.
 */

#ifndef HUSTLE_HUSTLEFRONTEND_H
#define HUSTLE_HUSTLEFRONTEND_H

#include <iostream>
#include <cstdio>

#include "sqlite3.h"

namespace hustle {
namespace frontend {

class HustleFrontend {
 public:
  // Creates a sqlite database with two tables R, S. Both tables have two
  // attributes named A and B.
  HustleFrontend();

  // Deletes the sqlite database we created during construction.
  ~HustleFrontend() {
    std::remove(kDBFileName.c_str());
  }

  // Convert an sql string to its explain output using sqlite.
  std::string SqlToPlan(const std::string &sql);
 private:

  // Sqlite3 database name
  const std::string kDBFileName = "test.db";
};

} // namespace frontend
} // namespace hustle

#endif //HUSTLE_HUSTLEFRONTEND_H
