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

#ifndef HUSTLE_REFERENCE_STRUCTS_H
#define HUSTLE_REFERENCE_STRUCTS_H

#include "storage/block.h"
#include "storage/table.h"

using namespace hustle::storage;

namespace hustle::operators {

struct RecordID {
  uint32_t index;
  uint16_t chunk;
};

struct ColumnReference {
  DBTable::TablePtr table;
  std::string col_name;

  bool operator==(const ColumnReference &col_ref) const {
    return table == col_ref.table && !col_name.compare(col_ref.col_name);
  }

  bool operator!=(const ColumnReference &col_ref) const {
    return table != col_ref.table || col_name.compare(col_ref.col_name);
  }
};

struct ExprReference {
  uint16_t op;
  std::shared_ptr<ExprReference> left_expr;
  std::shared_ptr<ExprReference> right_expr;
  std::shared_ptr<ColumnReference> column_ref;
};
}  // namespace hustle::operators

#endif  // HUSTLE_REFERENCE_STRUCTS_H