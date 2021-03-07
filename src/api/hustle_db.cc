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

void HustleDB::add_catalog(std::string db_name,
                           std::shared_ptr<Catalog> catalog) {
  auto catalog_itr = hustle::HustleDB::catalogs.find(db_name);
  if (catalog_itr != hustle::HustleDB::catalogs.end()) {
    return;
  }
  hustle::HustleDB::catalogs[db_name] = catalog;
}

std::shared_ptr<Catalog> HustleDB::get_catalog(std::string db_name) {
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
  utils::initialize_sqlite3();
    this->add_catalog(SqliteDBPath_, catalog_);
};

std::string HustleDB::get_plan(const std::string &sql) {
  return utils::execute_sqlite_result(SqliteDBPath_, sql);
}

bool HustleDB::create_table(const TableSchema ts) {
  return catalog_->AddTable(ts);
}

bool HustleDB::create_table(const TableSchema ts,
                            DBTable::TablePtr table_ref) {
  return catalog_->AddTable(ts, table_ref);
}

void HustleDB::load_tables() {
    utils::load_tables(SqliteDBPath_, hustle::HustleDB::catalogs[SqliteDBPath_]->GetTableNames());
}

std::string HustleDB::execute_query_result(const std::string &sql) {
  return utils::execute_sqlite_result(SqliteDBPath_, sql);
}

bool HustleDB::execute_query(const std::string &sql) {
  return utils::execute_sqlite_query(SqliteDBPath_, sql);
}

bool HustleDB::drop_table(const std::string &name) {
  return catalog_->DropTable(name);
}

bool HustleDB::drop_mem_table(const std::string &name) {
  return catalog_->DropMemTable(name);
}

}  // namespace hustle
