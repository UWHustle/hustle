#include "HustleDB.h"

#include <filesystem>

#include "catalog/Catalog.h"
#include "utils/sqlite_utils.h"

namespace hustle {

HustleDB::HustleDB(filesystem::path DBpath) :
    DBPath_(path),
    CatalogPath_(path + "catalog.db")
SqliteDBPath_(path
+ "hustle_sqlite.db") {

if(!
std::filesystem::exists(path)
) {
std::filesystem::create_directories(path);
}

catalog_ = catalog::Catalog::CreateCatalog(CatalogPath_, SqliteDBPath_);

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