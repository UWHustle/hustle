#include <stdio.h>
#include <filesystem>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"

#include "catalog/Catalog.h"
#include "api/HustleDB.h"

using namespace testing;
using hustle::catalog::ColumnType;
using hustle::catalog::HustleType;
using hustle::catalog::ColumnSchema;
using hustle::catalog::TableSchema;
using hustle::catalog::Catalog;


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

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(hustleDB.createTable(ts));

  EXPECT_TRUE(hustleDB.getCatalog()->TableExists(ts.getName()));
  EXPECT_FALSE(hustleDB.createTable(ts));

  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));

  hustle::HustleDB hustleDB2("db_directory");
  EXPECT_TRUE(hustleDB.getCatalog()->TableExists(ts.getName()));

  std::filesystem::remove_all("db_directory");
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
  EXPECT_TRUE(hustleDB.createTable(ts));

  EXPECT_TRUE(hustleDB.getCatalog()->TableExists(ts.getName()));

  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));

  EXPECT_TRUE(hustleDB.dropTable(ts.getName()));
  EXPECT_FALSE(hustleDB.getCatalog()->TableExists(ts.getName()));

  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/catalog.json"));
  EXPECT_TRUE(std::filesystem::exists("db_directory/db_dir_nested/hustle_sqlite.db"));

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
  EXPECT_TRUE(hustleDB.createTable(ts));

  std::string plan = hustleDB.getPlan("EXPLAIN QUERY PLAN SELECT c1,c2 FROM Subscriber;");

  EXPECT_EQ(plan, "SCAN TABLE Subscriber\n");

  std::filesystem::remove_all("db_directory");
}
