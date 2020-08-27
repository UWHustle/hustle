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

#include "table_schema.h"

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
  if (utils::contains<std::string, absl::flat_hash_map<std::string, int>>(
          c.getName(), name_to_id_)) {
    return false;
  }
  columns_.push_back(c);
  name_to_id_[c.getName()] = columns_.size() - 1;
  return true;
}

std::optional<ColumnSchema *> TableSchema::ColumnExists(std::string name) {
  if (!utils::contains<std::string, absl::flat_hash_map<std::string, int>>(
          name, name_to_id_)) {
    return std::nullopt;
  }
  return &columns_[name_to_id_[name]];
}

void TableSchema::print() const {
  std::cout << "-- Table: " << name_ << std::endl;
  for (const auto &c : columns_) {
    std::cout << "---- Column: " << c.toString() << std::endl;
  }
  std::cout << "---- Primary Key: ";
  for (const auto &c : primary_key_) {
    std::cout << c << ", ";
  }
  std::cout << std::endl;
}

}  // namespace catalog
}  // namespace hustle