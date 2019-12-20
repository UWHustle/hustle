#ifndef HUSTLE_TABLESCHEMA_H
#define HUSTLE_TABLESCHEMA_H

#include <optional>
#include <iostream>
#include "absl/hash/hash.h"
#include "absl/container/flat_hash_map.h"

#include "ColumnSchema.h"
#include "utils/map_utils.h"

namespace hustle {
namespace catalog {

class TableSchema {
 public:
  TableSchema(std::string name) : name_(name) {};

  TableSchema()  {};

  void setPrimaryKey(std::vector<std::string> pk) {
    primary_key_.clear();

    for (const auto &c : pk) {
      primary_key_.push_back(c);
    }
  }

  const std::vector<std::string>& getPrimaryKey() const {
    return primary_key_;
  }

  bool addColumn(ColumnSchema c) {
    if (utils::contains<std::string, absl::flat_hash_map<std::string, int>>(c.getName(), name_to_id_)) {
      return false;
    }
    columns_.push_back(c);
    name_to_id_[c.getName()] = columns_.size() - 1;
    return true;
  }

  std::optional<ColumnSchema*> ColumnExists(std::string name) {
    if (!utils::contains <std::string, absl::flat_hash_map<std::string, int>>
          (name, name_to_id_)) {
      return  std::nullopt;
    }
    return &columns_[name_to_id_[name]];
  }


  const std::vector<ColumnSchema>& getColumns() const {
    return columns_;
  }

  const std::string& getName() const {return name_;}

  void print() const {
    std::cout << "-- Table: " << name_ << std::endl;
    for (const auto& c : columns_) {
      std::cout << "---- Column: " << c.toString() << std::endl;
    }
    std::cout << "---- Primary Key: ";
    for (const auto& c : primary_key_) {
      std::cout << c << ", ";
    }
    std::cout << std::endl;
  }

  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(CEREAL_NVP(name_), CEREAL_NVP(columns_),
        CEREAL_NVP(primary_key_), CEREAL_NVP(name_to_id_));
  }

 private:
  std::string name_;
  std::vector<std::string> primary_key_;
  std::vector<ColumnSchema> columns_;
  absl::flat_hash_map<std::string, int> name_to_id_;
};

}
} // namespace hustle

#endif //HUSTLE_TABLESCHEMA_H
