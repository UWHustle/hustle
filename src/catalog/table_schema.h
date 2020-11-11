// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_TABLESCHEMA_H
#define HUSTLE_TABLESCHEMA_H

#include <iostream>
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "column_schema.h"
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

  const std::shared_ptr<arrow::Schema> getArrowSchema() const {
    return arrow::schema(fields_);
  }

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
  std::vector<std::shared_ptr<arrow::Field>> fields_;
  absl::flat_hash_map<std::string, int> name_to_id_;
};

}  // namespace catalog
}  // namespace hustle

#endif  // HUSTLE_TABLESCHEMA_H
