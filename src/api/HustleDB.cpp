#include "HustleDB.h"

#include <filesystem>

#include "catalog/Catalog.h"
#include "utils/sqlite_utils.h"

namespace hustle {

HustleDB::HustleDB(std::string DBpath) :
    DBPath_(DBpath),
    CatalogPath_(DBpath + "/" + "catalog.json"),
    SqliteDBPath_(DBpath + "/" + "hustle_sqlite.db"),
    catalog_(catalog::Catalog::CreateCatalog(CatalogPath_, SqliteDBPath_)) {

  if (!std::filesystem::exists(DBpath)) {
    std::filesystem::create_directories(DBpath);
  }
};

std::string HustleDB::getPlan(const std::string &sql) {
  return utils::executeSqliteReturnOutputString(SqliteDBPath_, sql);
}

bool HustleDB::createTable(const TableSchema ts) {
  return catalog_.addTable(ts);
}

bool HustleDB::dropTable(const std::string &name) {
  return catalog_.dropTable(name);
}

bool HustleDB::insert() {
  // TODO(nicholas) once arrow is integrated
  return true;
}

bool HustleDB::select() {
  // TODO(nicholas) once arrow is integrated
  return true;
}

} // namespace hustle