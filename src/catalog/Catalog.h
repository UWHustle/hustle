#ifndef HUSTLE_CATALOG_H
#define HUSTLE_CATALOG_H

#include <optional>
#include <iostream>
#include <sstream>
#include <memory>
#include <filesystem>

#include "sqlite3/sqlite3.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

#include "TableSchema.h"
#include "utils/map_utils.h"
#include "utils/sqlite_utils.h"
#include <cereal/access.hpp>
#include <cereal/archives/json.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/complex.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/memory.hpp>

namespace hustle {
namespace catalog {

class Catalog {
public:
  static Catalog CreateCatalog(std::string CatalogPath, std::string SqlitePath);

  bool addTable(TableSchema t);

  bool dropTable(std::string name);

  std::optional<TableSchema *> TableExists(std::string name);

  void print() const;

  int getTableIdbyName(const std::string &name) {
    return name_to_id_[name];
  }

  // Used by cereal for serialization/deserialization
  template<class Archive>
  void serialize(Archive &archive) {
    archive(
        CEREAL_NVP(CatalogPath_),
        CEREAL_NVP(SqlitePath_),
        CEREAL_NVP(name_to_id_),
        CEREAL_NVP(tables_));
  }

private:
  // Serializes and writes the catalog to a file in json format.
  void SaveToFile();

  // TODO(chronis) make private
  Catalog() {};

  Catalog(std::string CatalogPath, std::string SqlitePath) :
      CatalogPath_(CatalogPath), SqlitePath_(SqlitePath) {};

  std::vector<TableSchema> tables_;
  absl::flat_hash_map<std::string, int> name_to_id_;
  std::string CatalogPath_;
  std::string SqlitePath_;

  FRIEND_TEST(CatalogTest, Serialization);
};

Catalog CatalogFactory(std::string DBPath);

}
}

#endif //HUSTLE_CATALOG_H
