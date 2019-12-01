#include "gtest/gtest.h"
#include "sqlite3.h"

#include "../HustleFrontend.h"

// Test that the sqlite dabatase has been constructed succesfully.
TEST(HustleFrontend, DB_Construction) {

  hustle::frontend::HustleFrontend hustle_frontend;

  sqlite3 *db;
  int rc;

  rc = sqlite3_open("test.db", &db);

  EXPECT_EQ(rc, 0);
}


