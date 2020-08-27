#ifndef HUSTLE_COLUMNSCHEMA_H
#define HUSTLE_COLUMNSCHEMA_H
#include <cereal/archives/json.hpp>
#include <fstream>
#include <string>

#include "absl/strings/str_cat.h"

namespace hustle {
namespace catalog {

enum HustleType { INTEGER, CHAR };

class ColumnType {
 public:
  ColumnType(HustleType type) : type_(type), size_(std::nullopt){};

  ColumnType(HustleType type, int size) : type_(type), size_(size){};

  // TODO(chronis) make private
  ColumnType(){};

  bool has_size() {
    if (size_) {
      return true;
    }
    return false;
  }

  const HustleType &getHustleType() const { return type_; }

  const std::optional<int> &getSize() const { return size_; }

  std::string toString() const {
    switch (type_) {
      case HustleType::INTEGER:
        return "INT";
        break;
      case HustleType::CHAR:
        return absl::StrCat("CHAR(", std::to_string(*size_) + ")");
        break;
      default:
        return "wrong type";
    }
  }

  std::string toSQLString() const {
    switch (type_) {
      case HustleType::INTEGER:
        return "INTEGER";
        break;
      case HustleType::CHAR:
        return absl::StrCat("CHAR(", std::to_string(*size_) + ")");
        break;
    }
  }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(CEREAL_NVP(type_), CEREAL_NVP(size_));
  }

 private:
  // Not const because of cereal serialization
  HustleType type_;
  std::optional<int> size_;
};

class ColumnSchema {
 public:
  ColumnSchema(std::string name, ColumnType type, bool notNull, bool unique)
      : name_(name), type_(type), notNull_(notNull), unique_(unique){};

  // TODO(chronis) make private
  ColumnSchema(){};

  const std::string &getName() const { return name_; }
  const ColumnType &getType() const { return type_; }
  const HustleType &getHustleType() const { return type_.getHustleType(); }
  const std::optional<int> &getTypeSize() const { return type_.getSize(); }
  const bool isUnique() const { return unique_; }
  const bool isNotNull() const { return notNull_; }

  std::string toString() const {
    return name_ + " " + type_.toString() +
           " notNull: " + std::to_string(notNull_) +
           " unique: " + std::to_string(unique_);
  }

  template <class Archive>
  void serialize(Archive &archive) {
    archive(CEREAL_NVP(name_), CEREAL_NVP(type_), CEREAL_NVP(notNull_),
            CEREAL_NVP(unique_));
  }

 private:
  // Not const because of cereal serialization
  std::string name_;
  ColumnType type_;
  bool notNull_;
  bool unique_;
};

}  // namespace catalog
}  // namespace hustle

#endif  // HUSTLE_COLUMNSCHEMA_H
