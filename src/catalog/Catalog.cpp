#include "Catalog.h"

#include <string>
#include <iostream>
#include <filesystem>

namespace hustle {
namespace catalog {

namespace {
// Creates the sql statement tom create the table described by TableSchema ts
std::string createCreateSql(const TableSchema &ts) {
  std::string sql;
  absl::StrAppend(&sql, "CREATE TABLE ", ts.getName(), "(");

  for (const auto &c : ts.getColumns()) {
    absl::StrAppend(&sql, c.getName(), " ", c.getType().toString(), " ");
    if (c.isUnique()) { absl::StrAppend(&sql, " UNIQUE "); }
    if (c.isNotNull()) { absl::StrAppend(&sql, " NOT NULL "); }
    absl::StrAppend(&sql, ", ");
  }

  absl::StrAppend(&sql, "PRIMARY KEY(");
  if (ts.getPrimaryKey().size() >= 1) {
    absl::StrAppend(&sql, ts.getPrimaryKey()[0]);
    for (int i = 1; i < ts.getPrimaryKey().size(); ++i) {
      absl::StrAppend(&sql, ", ", ts.getPrimaryKey()[i]);
    }
  }
  absl::StrAppend(&sql, "));");

  return sql;
}
}

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
      utils::executeSqliteNoOutput(CatalogPath,
                                   createCreateSql(t));
    }
    return catalog;
  } else {
    return Catalog(CatalogPath, SqlitePath);
  }
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

bool Catalog::dropTable(std::string name) {
  auto search = name_to_id_.find(name);
  if (search == name_to_id_.end()) {
    return false;
  }

  tables_.erase(tables_.begin());
  name_to_id_.erase(search);

  SaveToFile();

  if (!utils::executeSqliteNoOutput(SqlitePath_,
                                    absl::StrCat("DROP TABLE ", name, ";"))) {
    std::cerr << "SqliteDB catalog out of sync" << std::endl;
  }
  return true;
}

std::optional<TableSchema *> Catalog::TableExists(std::string name) {
  auto search = name_to_id_.find(name);
  if (search == name_to_id_.end()) {
    return std::nullopt;
  }

  return &tables_[name_to_id_[name]];
}

bool Catalog::addTable(TableSchema t) {
  auto search = name_to_id_.find(t.getName());
  if (search != name_to_id_.end()) {
    return false;
  }

  tables_.push_back(t);
  name_to_id_[t.getName()] = tables_.size() - 1;

  SaveToFile();

  if (!utils::executeSqliteNoOutput(SqlitePath_,
                                    createCreateSql(t))) {
    std::cerr << "SqliteDB catalog out of sync" << std::endl;
  }
  return true;
}

void Catalog::print() const {
  std::cout << "----------- DATABASE CATALOG --------" << std::endl;
  std::cout << "Catalog File Path: " << CatalogPath_ << std::endl;
  std::cout << "Sqlite Catalog File Path: " << SqlitePath_ << std::endl;
  for (const auto &t : tables_) {
    t.print();
  }
  std::cout << "----------- ----------- --------" << std::endl;
}

} // namespace catalog
} // namespace hustle