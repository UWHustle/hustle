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

#include "hustle_db.h"

#include <filesystem>

#include "catalog/catalog.h"
#include "scheduler/scheduler.h"
#include "utils/sqlite_utils.h"

namespace hustle {

std::map<std::string, std::shared_ptr<Catalog>> hustle::HustleDB::catalogs = {};

void HustleDB::addCatalog(std::string db_name,
                          std::shared_ptr<Catalog> catalog) {
  auto catalog_itr = hustle::HustleDB::catalogs.find(db_name);
  if (catalog_itr != hustle::HustleDB::catalogs.end()) {
    return;
  }
  hustle::HustleDB::catalogs[db_name] = catalog;
}

std::shared_ptr<Catalog> HustleDB::getCatalog(std::string db_name) {
  return hustle::HustleDB::catalogs[db_name];
}

HustleDB::HustleDB(std::string DBpath)
    : DBPath_(DBpath),
      CatalogPath_(DBpath + "/" + "catalog.json"),
      SqliteDBPath_(DBpath + "/" + "hustle_sqlite.db"),
      catalog_(
          catalog::Catalog::CreateCatalogObject(CatalogPath_, SqliteDBPath_)) {
  if (!std::filesystem::exists(DBpath)) {
    std::filesystem::create_directories(DBpath);
  }
  this->addCatalog(SqliteDBPath_, catalog_);
  Scheduler::GlobalInstance().start();
};

std::string HustleDB::getPlan(const std::string &sql) {
  return utils::executeSqliteReturnOutputString(SqliteDBPath_, sql);
}

bool HustleDB::createTable(const TableSchema ts) {
  return catalog_->addTable(ts);
}

bool HustleDB::createTable(const TableSchema ts,
                           std::shared_ptr<DBTable> table_ref) {
  return catalog_->addTable(ts, table_ref);
}

std::string HustleDB::executeQuery(const std::string &sql) {
  return utils::executeSqliteReturnOutputString(SqliteDBPath_, sql);
}

bool HustleDB::dropTable(const std::string &name) {
  return catalog_->dropTable(name);
}

bool HustleDB::insert() {
  // TODO(nicholas) once arrow is integrated
  return true;
}

bool HustleDB::select() {
  // TODO(nicholas) once arrow is integrated
  return true;
}

HustleDB::~HustleDB() {
  Scheduler::GlobalInstance().join();
}

}  // namespace hustle