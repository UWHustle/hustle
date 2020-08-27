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

#include <string>

#include "absl/strings/str_cat.h"
#include "catalog/catalog.h"
#include "catalog/table_schema.h"

using hustle::catalog::Catalog;
using hustle::catalog::TableSchema;

namespace hustle {

class HustleDB {
 public:
  HustleDB(std::string path);

  bool createTable(const TableSchema ts);

  bool dropTable(const std::string &name);

  std::string getPlan(const std::string &sql);

  // Not implemented yet.
  bool insert();

  // Not implemented yet.
  bool select();

  Catalog *getCatalog() { return &catalog_; }

 private:
  const std::string DBPath_;
  const std::string CatalogPath_;
  const std::string SqliteDBPath_;
  Catalog catalog_;
};

}  // namespace hustle

#endif  // HUSTLE_HUSTLEDB_H
