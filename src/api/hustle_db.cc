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
  this->add_catalog(SqliteDBPath_, catalog_);
    hustle_memlog_initialize(&this->memlog, SqliteDBPath_.c_str(), MEMLOG_INIT_SIZE);
    //utils::open_sqlite3_db(SqliteDBPath_, &db);
};

std::string HustleDB::get_plan(const std::string &sql) {
  return utils::execute_sqlite_result(db, sql);
}

bool HustleDB::create_table(const TableSchema ts) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    auto result = catalog_->AddTable(local_db, ts);
    utils::close_sqlite3(local_db);
    return result;
}

bool HustleDB::create_table(const TableSchema ts, DBTable::TablePtr table_ref) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    auto result =  catalog_->AddTable(local_db, ts, table_ref);
    utils::close_sqlite3(local_db);
    return result;
}

void HustleDB::reinitialize_sqlite_db() {
  if (db != NULL) {
    utils::close_sqlite3(db);
  }
  utils::open_sqlite3_db(SqliteDBPath_, &db, this->memlog);
}

void HustleDB::load_tables() {
  this->reinitialize_sqlite_db();
  utils::load_tables(
      db, hustle::HustleDB::catalogs[SqliteDBPath_]->GetTableNames());
}

std::string HustleDB::execute_query_result(const std::string &sql) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    std::string result = utils::execute_sqlite_result(local_db, sql);
    utils::close_sqlite3(local_db);
    return result;
}

bool HustleDB::execute_query(const std::string &sql) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    bool result =  utils::execute_sqlite_query(local_db, sql);
    utils::close_sqlite3(local_db);
    return result;
}

bool HustleDB::drop_table(const std::string &name) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    auto result =  catalog_->DropTable(local_db, name);
    utils::close_sqlite3(local_db);
    return result;
}

bool HustleDB::drop_mem_table(const std::string &name) {
    sqlite3 *local_db;
    utils::open_sqlite3_db(SqliteDBPath_, &local_db, this->memlog);
    auto result =  catalog_->DropMemTable(name);
    utils::close_sqlite3(local_db);
    return result;
}

}  // namespace hustle
