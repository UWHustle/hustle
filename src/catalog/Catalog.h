#ifndef HUSTLE_CATALOG_H
#define HUSTLE_CATALOG_H

#include <optional>
#include <iostream>
#include <sstream>
#include <memory>

#include "sqlite3.h"
#include "absl/strings/str_cat.h"

#include "TableSchema.h"
#include "utils/map_utils.h"
#include <cereal/access.hpp>
#include <cereal/types/memory.hpp>


namespace hustle {
namespace catalog {

namespace {

class SqliteCatalog {
 public:
  SqliteCatalog(std::string path) : sqlitepath_(path) {};
  SqliteCatalog() : sqlitepath_("default_path") {};

  bool addTable(const TableSchema& t) {
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    char *sql;

    rc = sqlite3_open(sqlitepath_.c_str(), &db);

    if( rc ) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      return false;
    } else {
      fprintf(stdout, "Opened database successfully\n");
    }

    rc = sqlite3_exec(db, createCreateSql(t).c_str(), nullptr, 0, &zErrMsg);

    if( rc != SQLITE_OK ){
      fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      return false;
    } else {
      fprintf(stdout, "Table created successfully\n");
    }
    sqlite3_close(db);

    return true;
  }

  bool dropTable(const std::string& name) {
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    char *sql;

    rc = sqlite3_open(sqlitepath_.c_str(), &db);

    if( rc ) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      return false;
    } else {
      fprintf(stdout, "Opened database successfully\n");
    }

    rc = sqlite3_exec(db, absl::StrCat("DROP TABLE ",name,";").c_str(), nullptr, 0, &zErrMsg);

    if( rc != SQLITE_OK ){
      fprintf(stderr, "SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      return false;
    } else {
      fprintf(stdout, "Table created successfully\n");
    }
    sqlite3_close(db);

    return true;
  }

  std::string getPath() {return sqlitepath_;}
 private:
  std::string createCreateSql(const TableSchema& ts) const {

    std::string sql;
    absl::StrAppend(&sql, "CREATE TABLE ", ts.getName(), "(");

    for (const auto& c : ts.getColumns()) {
      absl::StrAppend(&sql, c.getName(), " ", c.getType().toString(), " ");
      if (c.isUnique()) { absl::StrAppend(&sql, " UNIQUE " ); }
      if (c.isNotNull()) { absl::StrAppend(&sql, " NOT NULL " ); }
      absl::StrAppend(&sql, ", ");
    }

    absl::StrAppend(&sql, "PRIMARY KEY(");
    for (const auto& name : ts.getPrimaryKey()) {
      absl::StrAppend(&sql, name, ",");
    }
    absl::StrAppend(&sql, ");");

    return sql;
  }

  const std::string sqlitepath_;
};


}


class Catalog {
 public:
  Catalog( std::string catalogPath) :
      catalogPath_(catalogPath)
      {};

  Catalog( std::string catalogPath, std::string a) :
      catalogPath_(catalogPath)
  {};

  // TODO(chronis) make private
  Catalog() {};

  bool addTable(TableSchema t) {
    if (utils::contains<std::string, absl::flat_hash_map<std::string, int>>
          (t.getName(), name_to_id_)) {
      return false;
    }
//    auto search = name_to_id_.find(t.getName());
//    if (search != name_to_id_.end()) {
//      return false;
//    }
    tables_.push_back(t);
    name_to_id_[t.getName()] =  tables_.size() - 1;

//    sqliteCatalog_->addTable(t);
    return true;
  }

  bool dropTable(std::string name) {
    auto search = name_to_id_.find(name);
    if (search == name_to_id_.end()) {
      return false;
    }

    tables_.erase(tables_.begin());//tables_.begin() + search->second);
    name_to_id_.erase(search);

//    sqliteCatalog_->dropTable(name);
    return true;
  }

  std::optional<TableSchema*> TableExists(std::string name) {
    if (!utils::contains <std::string, absl::flat_hash_map<std::string, int>>
        (name, name_to_id_)) {
      return  std::nullopt;
    }
    return &tables_[name_to_id_[name]];
  }

  void print() const {
    std::cout << "----------- DATABASE CATALOG --------" << std::endl;
    std::cout << "Catalog File Path: " << catalogPath_ << std::endl;
    for (const auto& t : tables_) {
      t.print();
    }
    std::cout << "----------- ----------- --------" << std::endl;
  }

  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(CEREAL_NVP(catalogPath_),
            CEREAL_NVP(name_to_id_),
            CEREAL_NVP(tables_));
  }

 private:
  std::vector<TableSchema> tables_;
  absl::flat_hash_map<std::string, int> name_to_id_;
//  std::unique_ptr<SqliteCatalog> sqliteCatalog_;
  std::string catalogPath_;
};
//
//std::unique_ptr<Catalog> DeserializeCatalog() {
//
//}
//
//SerializeCatalog


}
}

#endif //HUSTLE_CATALOG_H
