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

#ifndef HUSTLE_HUSTLEDB_H
#define HUSTLE_HUSTLEDB_H

#include <memory>
#include <string>

#include "catalog/catalog.h"
#include "catalog/table_schema.h"
#include "scheduler/scheduler.h"

using hustle::catalog::Catalog;
using hustle::catalog::TableSchema;

namespace hustle {

class HustleDB {
 public:
  static std::map<std::string, std::shared_ptr<Catalog>> catalogs;

  static Scheduler &get_scheduler() { return Scheduler::GlobalInstance(); }

  static void add_catalog(std::string db_name,
                          std::shared_ptr<Catalog> catalog);

  static std::shared_ptr<Catalog> get_catalog(std::string db_name);

  static void init() {
    if (!Scheduler::GlobalInstance().isActive()) {
      Scheduler::GlobalInstance().start();
    }
    utils::init_sqlite3();
  }

  static void destroy() {
    if (Scheduler::GlobalInstance().isActive()) {
      Scheduler::GlobalInstance().join();
    }
    catalogs.clear();
    utils::destroy_sqlite3();
  }

  HustleDB(std::string path);

  void reinitialize_sqlite_db();

  bool create_table(const TableSchema ts);

  bool create_table(const TableSchema ts, DBTable::TablePtr table_ref);

  void load_tables();

  bool drop_table(const std::string &name);

  bool drop_mem_table(const std::string &name);

  std::string execute_query_result(const std::string &sql);

  bool execute_query(const std::string &sql);

  std::string get_plan(const std::string &sql);

  inline sqlite3 *sqlite3_db() { return db; }

  inline const std::string get_sqlite_path() { return SqliteDBPath_; }

  std::shared_ptr<Catalog> get_catalog() { return catalog_; }

  ~HustleDB() { utils::close_sqlite3(db); }

 private:
  sqlite3 *db;
  const std::string DBPath_;
  const std::string CatalogPath_;
  const std::string SqliteDBPath_;
  std::shared_ptr<Catalog> catalog_;
};

}  // namespace hustle

#endif  // HUSTLE_HUSTLEDB_H
