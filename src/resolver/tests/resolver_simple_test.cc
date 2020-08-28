// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "resolver/resolver.h"
#include "sqlite3/sqlite3.h"

using namespace testing;
using nlohmann::json;

class ResolverSimpleTest : public Test {
  void SetUp() override {
    /**
      CREATE TABLE Subscriber(c1 INT NOT NULL , c2 INT UNIQUE);
      CREATE TABLE AccessInfo(c3 INT NOT NULL , c4 INT UNIQUE);
     */

    std::filesystem::remove_all("db_directory");
    EXPECT_FALSE(std::filesystem::exists("db_directory"));

    hustle::HustleDB hustleDB("db_directory");

    // Create table Subscriber
    hustle::catalog::TableSchema ts("Subscriber");
    hustle::catalog::ColumnSchema c1(
        "c1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema c2(
        "c2", {hustle::catalog::HustleType::INTEGER, 0}, false, true);
    ts.addColumn(c1);
    ts.addColumn(c2);
    ts.setPrimaryKey({});

    hustleDB.createTable(ts);

    // Create table AccessInfo
    hustle::catalog::TableSchema ts1("AccessInfo");
    hustle::catalog::ColumnSchema c3(
        "c3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema c4(
        "c4", {hustle::catalog::HustleType::INTEGER, 0}, false, true);
    ts1.addColumn(c3);
    ts1.addColumn(c4);
    ts1.setPrimaryKey({});

    hustleDB.createTable(ts1);
  }
};

TEST_F(ResolverSimpleTest, test1) {
  hustle::HustleDB hustleDB("db_directory");

  std::string query =
      "EXPLAIN QUERY PLAN select Subscriber.c1 "
      "from Subscriber, AccessInfo "
      "where Subscriber.c1 = AccessInfo.c3;";

  hustleDB.getPlan(query);

  auto parser = std::make_shared<hustle::parser::Parser>();
  auto resolver =
      std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
  parser->parse(query, hustleDB);
  resolver->resolve(parser->getParseTree());
  std::cout << "Plan:" << resolver->toString(4) << std::endl;

  // TODO(Lichengxi): build validation plan
}

TEST_F(ResolverSimpleTest, test2) {
  hustle::HustleDB hustleDB("db_directory");

  std::string query =
      "EXPLAIN QUERY PLAN select Subscriber.c1 "
      "from Subscriber, AccessInfo "
      "where Subscriber.c1 = AccessInfo.c3 and Subscriber.c2 > 2 and "
      "AccessInfo.c4 < 5;";

  hustleDB.getPlan(query);

  auto parser = std::make_shared<hustle::parser::Parser>();
  auto resolver =
      std::make_shared<hustle::resolver::Resolver>(hustleDB.getCatalog());
  parser->parse(query, hustleDB);
  resolver->resolve(parser->getParseTree());
  std::cout << "Plan:" << resolver->toString(4) << std::endl;

  // TODO(Lichengxi): build validation plan
}
