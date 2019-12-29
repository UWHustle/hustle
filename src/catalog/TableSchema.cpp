#include "TableSchema.h"

#include "utils/map_utils.h"

namespace hustle {
namespace catalog {

void TableSchema::setPrimaryKey(std::vector<std::string> pk) {
  primary_key_.clear();

  for (const auto &c : pk) {
    primary_key_.push_back(c);
  }
}

bool TableSchema::addColumn(ColumnSchema c) {
  if (utils::contains<std::string, absl::flat_hash_map<std::string, int>>(c.getName(), name_to_id_)) {
    return false;
  }
  columns_.push_back(c);
  name_to_id_[c.getName()] = columns_.size() - 1;
  return true;
}

std::optional<ColumnSchema*> TableSchema::ColumnExists(std::string name) {
  if (!utils::contains <std::string, absl::flat_hash_map<std::string, int>>
      (name, name_to_id_)) {
    return  std::nullopt;
  }
  return &columns_[name_to_id_[name]];
}

void TableSchema::print() const {
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


}
}