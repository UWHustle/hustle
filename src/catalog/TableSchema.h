#ifndef HUSTLE_TABLESCHEMA_H
#define HUSTLE_TABLESCHEMA_H

#include <iostream>
#include <optional>

#include "ColumnSchema.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "utils/map_utils.h"

namespace hustle {
namespace catalog {

class TableSchema {
 public:
  TableSchema(std::string name) : name_(name){};

  // TODO(chronis) Make private
  TableSchema(){};

  void setPrimaryKey(std::vector<std::string> pk);

  const std::vector<std::string>& getPrimaryKey() const { return primary_key_; }

  bool addColumn(ColumnSchema c);

  std::optional<ColumnSchema*> ColumnExists(std::string name);

  const std::vector<ColumnSchema>& getColumns() const { return columns_; }

  const std::string& getName() const { return name_; }

  void print() const;

  template <class Archive>
  void serialize(Archive& archive) {
    archive(CEREAL_NVP(name_), CEREAL_NVP(columns_), CEREAL_NVP(primary_key_),
            CEREAL_NVP(name_to_id_));
  }

 private:
  std::string name_;
  std::vector<std::string> primary_key_;
  std::vector<ColumnSchema> columns_;
  absl::flat_hash_map<std::string, int> name_to_id_;
};

}  // namespace catalog
}  // namespace hustle

#endif  // HUSTLE_TABLESCHEMA_H
