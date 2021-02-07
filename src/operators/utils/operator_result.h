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

#ifndef HUSTLE_OPERATORRESULT_H
#define HUSTLE_OPERATORRESULT_H

#include <arrow/compute/api.h>

#include <string>

#include "operators/utils/lazy_table.h"
#include "storage/block.h"
#include "storage/table.h"

using namespace hustle::storage;

namespace hustle::operators {

enum FilterOperator {
  AND,
  OR,
  NONE,
};

class OperatorResult {
 public:
  using OpResultPtr = std::shared_ptr<OperatorResult>;
  /**
   * Construct an OperatorResult, the type that all operators return. It is
   * simply a collection of LazyTables. Since a LazyTable can be thought
   * of as a "view" for a table, an OperatorResult can be thought of as a
   * collection of "views" across multiple tables.
   *
   * @param lazy_tables a vector of LazyTables
   */
  explicit OperatorResult(std::vector<LazyTable> lazy_tables);

  /**
   * Construct an empty OperatorResult.
   */
  OperatorResult();

  /**
   * Append a new table to the OperatorResult. Internally, we wrap the
   * table parameter in a LazyTable with empty filter and indices before
   * appending.
   *
   * @param table The table to append
   */
  void append(DBTable::TablePtr table);

  /**
   * Append a new lazy table to the OperatorResult.
   * @param table The lazy table to append
   */
  void append(LazyTable lazy_table);

  /**
   * Append a new lazy table to the OperatorResult.
   * @param table Shared pointer to the lazy table to append
   */
  void append(std::shared_ptr<LazyTable> lazy_table);

  /**
   * Append another OperatorResult to this OperatorResult.
   *
   * @param result Another OperatorResult
   */
  void append(const std::shared_ptr<OperatorResult>& result);

  /**
   * Get the LazyTable at index i
   *
   * @param i
   * @return a LazyTable
   */
  LazyTable get_table(int i);

  /**
   * Get the LazyTable that matches a particular table.
   *
   * @param table
   * @return a LazyTable
   */
  LazyTable get_table(const DBTable::TablePtr& table);

  /**
   * Construct a new table from the OperatorResult
   *
   * @param col_refs References to the columns to project in the output table.
   * @return A new table containing all columns specified by col_refs
   */
  DBTable::TablePtr materialize(
      const std::vector<ColumnReference>& col_refs);

  std::vector<LazyTable> lazy_tables_;

  void set_materialized_col(DBTable::TablePtr table, int i,
                            std::shared_ptr<arrow::ChunkedArray> col);
};

}  // namespace hustle::operators

#endif  // HUSTLE_OPERATORRESULT_H
