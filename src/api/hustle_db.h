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

  static Scheduler &getScheduler() { return Scheduler::GlobalInstance(); }

  static void addCatalog(std::string db_name, std::shared_ptr<Catalog> catalog);

  static std::shared_ptr<Catalog> getCatalog(std::string db_name);

  HustleDB(std::string path);

  bool createTable(const TableSchema ts);

  bool createTable(const TableSchema ts, std::shared_ptr<DBTable> table_ref);

  void loadTables();

  bool dropTable(const std::string &name);

  bool clearMemTable(const std::string &name);

  std::string executeQuery(const std::string &sql);

  bool executeNoOutputQuery(const std::string &sql);

  std::string getPlan(const std::string &sql);

  const std::string getSqliteDBPath() { return SqliteDBPath_; }
  // Not implemented yet.
  bool insert();

  // Not implemented yet.
  bool select();

  static bool startScheduler() {
    if (!Scheduler::GlobalInstance().isActive()) {
      Scheduler::GlobalInstance().start();
      return true;
    }
    return false;
  }

  static bool stopScheduler() {
    if (Scheduler::GlobalInstance().isActive()) {
      Scheduler::GlobalInstance().join();
      return true;
    }
    return false;
  }

  Catalog *getCatalog() { return catalog_.get(); }

   ~HustleDB() {}

 private:
  const std::string DBPath_;
  const std::string CatalogPath_;
  const std::string SqliteDBPath_;
  std::shared_ptr<Catalog> catalog_;
};

}  // namespace hustle

#endif  // HUSTLE_HUSTLEDB_H
