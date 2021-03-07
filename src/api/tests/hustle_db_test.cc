#include "api/hustle_db.h"

#include <stdio.h>

#include <filesystem>

#include "catalog/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "sqlite3/sqlite3.h"

using namespace testing;
using hustle::catalog::Catalog;
using hustle::catalog::ColumnSchema;
using hustle::catalog::ColumnType;
using hustle::catalog::HustleType;
using hustle::catalog::TableSchema;

TEST(HustleDB, Create) {
  std::filesystem::remove_all("db_directory");

  EXPECT_FALSE(std::filesystem::exists("db_directory"));

  hustle::HustleDB hustleDB("db_directory");

  EXPECT_TRUE(std::filesystem::exists("db_directory"));

  std::filesystem::remove_all("db_directory");
}

TEST(HustleDB, CreateDBnestedDirectories) {
  std::filesystem::remove_all("db_directory2");

  EXPECT_FALSE(std::filesystem::exists("db_directory2/db_dir_nested"));

  hustle::HustleDB hustleDB2("db_directory2/db_dir_nested");

  EXPECT_TRUE(std::filesystem::exists("db_directory2/db_dir_nested"));

  std::filesystem::remove_all("db_directory2");
}

TEST(HustleDB, createTable) {
  std::filesystem::remove_all("db_directory");

  hustle::HustleDB hustleDB("db_directory/db_dir_nested");
    hustle::HustleDB::start_scheduler();

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(hustleDB.create_table(ts));

  EXPECT_TRUE(hustleDB.get_catalog()->TableExists(ts.getName()));
  EXPECT_FALSE(hustleDB.create_table(ts));

  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));

  hustle::HustleDB hustleDB2("db_directory");
  EXPECT_TRUE(hustleDB.get_catalog()->TableExists(ts.getName()));

  std::filesystem::remove_all("db_directory");
    hustle::HustleDB::stop_scheduler();
}

TEST(HustleDB, DropTable) {
  std::filesystem::remove_all("db_directory");

  hustle::HustleDB hustleDB("db_directory/db_dir_nested");

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(hustleDB.create_table(ts));

  EXPECT_TRUE(hustle::HustleDB::get_catalog("db_directory/db_dir_nested/hustle_sqlite.db")->TableExists(ts.getName()));

  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));
  
  EXPECT_TRUE(hustleDB.drop_table(ts.getName()));
  EXPECT_FALSE(hustle::HustleDB::get_catalog("db_directory/db_dir_nested/hustle_sqlite.db")->TableExists(ts.getName()));

  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(
      std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));

  std::filesystem::remove_all("db_directory");
}

TEST(HustleDB, getPlan) {
  std::filesystem::remove_all("db_directory");

  hustle::HustleDB hustleDB("db_directory/db_dir_nested");

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(hustleDB.create_table(ts));

  std::string plan =
          hustleDB.get_plan("EXPLAIN QUERY PLAN SELECT c1,c2 FROM Subscriber;");

  EXPECT_EQ(plan, "2 | 0 | 0 | SCAN TABLE Subscriber\n");

  std::filesystem::remove_all("db_directory");
}
