#ifndef HUSTLE_HUSTLEDB_H
#define HUSTLE_HUSTLEDB_H

#include <string>

#include "absl/strings/str_cat.h"
#include "catalog/TableSchema.h"
#include "catalog/Catalog.h"

using hustle::catalog::TableSchema;
using hustle::catalog::Catalog;

namespace hustle {

class HustleDB {
 public:
  HustleDB(std::string path);

  bool createTable(const TableSchema ts);

  bool dropTable(const std::string &name);

  std::string getPlan(const std::string &sql);

  bool insert();

  bool select();

  Catalog *getCatalog() { return &catalog_; }

 private:
  const std::string DBPath_;
  const std::string CatalogPath_;
  const std::string SqliteDBPath_;
  Catalog catalog_;
};

} // namespace hustle

#endif //HUSTLE_HUSTLEDB_H
