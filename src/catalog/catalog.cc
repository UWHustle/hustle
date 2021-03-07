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

#include "catalog.h"

#include <filesystem>
#include <iostream>
#include <string>

namespace hustle {
namespace catalog {

namespace {
// Creates the sql statement tom create the table described by TableSchema ts
std::string createCreateSql(const TableSchema &ts) {
  std::string sql;
  absl::StrAppend(&sql, "BEGIN;");
  absl::StrAppend(&sql, "CREATE TABLE ", ts.getName(), "(");

  const std::vector<ColumnSchema> &cols = ts.getColumns();
  for (int i = 0; i < cols.size(); i++) {
    auto &c = cols[i];
    absl::StrAppend(&sql, c.getName(), " ", c.getType().toString(), " ");
    if (c.isUnique()) {
      absl::StrAppend(&sql, " UNIQUE");
    }
    if (c.isNotNull()) {
      absl::StrAppend(&sql, " NOT NULL");
    }
    if (i < cols.size() - 1) {
      absl::StrAppend(&sql, ", ");
    }
  }

  if (ts.getPrimaryKey().size() >= 1) {
    absl::StrAppend(&sql, ", PRIMARY KEY(");
    absl::StrAppend(&sql, ts.getPrimaryKey()[0]);
    for (int i = 1; i < ts.getPrimaryKey().size(); ++i) {
      absl::StrAppend(&sql, ", ", ts.getPrimaryKey()[i]);
    }
    absl::StrAppend(&sql, ")");
  }
  absl::StrAppend(&sql, ");");
  absl::StrAppend(&sql, "COMMIT;");

  return sql;
}
}  // namespace

Catalog Catalog::CreateCatalog(std::string CatalogPath,
                               std::string SqlitePath) {
  if (std::filesystem::exists(CatalogPath)) {
    // Delete the sqlite DB which will be re-created.
    // The sqlite db is used to produce execution plans, only its catalog
    // is kept updated.
    std::filesystem::remove(SqlitePath);

    Catalog catalog;
    std::ifstream in(CatalogPath);
    {
      cereal::JSONInputArchive iarchive(in);
      iarchive(catalog);
    }
    // Rebuild the sqlite db catalog.
    for (const auto &t : catalog.tables_) {
        utils::execute_sqlite_query(SqlitePath, createCreateSql(t.second.table_schema));
    }
    return catalog;
  } else {
    return Catalog(CatalogPath, SqlitePath);
  }
}

std::shared_ptr<Catalog> Catalog::CreateCatalogObject(std::string CatalogPath,
                                                      std::string SqlitePath) {
  return std::make_shared<Catalog>(CatalogPath, SqlitePath);
}

DBTable::TablePtr Catalog::GetTable(size_t table_id) {
  // TODO: maintain a structure for storing tables in the db
  return nullptr;
}

DBTable::TablePtr Catalog::GetTable(std::string table_name) {
  auto search = tables_.find(table_name);
  if (search == tables_.end()) {
    return nullptr;
  }
  return tables_[table_name].table;
}

void Catalog::SaveToFile() {
  std::stringstream ss;
  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(*this);
  }

  std::ofstream out(CatalogPath_);
  out << ss.str() << std::endl;
  out.close();
}

bool Catalog::DropTable(std::string name) {
  if (!utils::execute_sqlite_query(SqlitePath_,
                                   absl::StrCat("DROP TABLE ", name, ";"))) {
    std::cerr << "SqliteDB catalog out of sync" << std::endl;
    return false;
  }
  return true;
}

bool Catalog::DropMemTable(std::string name) {
  auto search = tables_.find(name);
  if (search == tables_.end()) {
    return false;
  }
  tables_.erase(search);
  SaveToFile();
  return true;
}

std::optional<TableSchema *> Catalog::TableExists(std::string name) {
  auto search = tables_.find(name);
  if (search == tables_.end()) {
    return std::nullopt;
  }

  return &tables_[name].table_schema;
}

bool Catalog::AddTable(TableSchema t) {
  return this->AddTable(t, nullptr);
}

bool Catalog::AddTable(TableSchema t, DBTable::TablePtr table_ref) {
 auto search = tables_.find(t.getName());
  if (search != tables_.end()) {
    return false;
  }

  tables_[t.getName()] = {t, table_ref};
  SaveToFile();

  if (!utils::execute_sqlite_query(SqlitePath_, createCreateSql(t))) {
    std::cerr << "SqliteDB catalog out of sync" << std::endl;
  }
  return true;
}

void Catalog::print() const {
  std::cout << "----------- DATABASE CATALOG --------" << std::endl;
  std::cout << "Catalog File Path: " << CatalogPath_ << std::endl;
  std::cout << "Sqlite Catalog File Path: " << SqlitePath_ << std::endl;
  for (const auto &t : tables_) {
    t.second.table_schema.print();
  }
  std::cout << "----------- ----------- --------" << std::endl;
}

}  // namespace catalog
}  // namespace hustle