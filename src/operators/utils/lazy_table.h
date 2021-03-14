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

#ifndef HUSTLE_LAZYTABLE_H
#define HUSTLE_LAZYTABLE_H

#include <arrow/compute/api.h>
#include <utils/parallel_hashmap/phmap.h>

#include <string>

#include "scheduler/task.h"
#include "storage/block.h"
#include "storage/table.h"
#include "utils/arrow_compute_wrappers.h"

using namespace hustle::storage;

namespace hustle::operators {

struct RecordID {
  uint32_t index;
  uint16_t chunk;
};

struct ColumnReference {
  DBTable::TablePtr table;
  std::string col_name;
};

struct ExprReference {
  uint16_t op;
  std::shared_ptr<ExprReference> left_expr;
  std::shared_ptr<ExprReference> right_expr;
  std::shared_ptr<ColumnReference> column_ref;
};

/**
 * A LazyTable associates a table pointer with a boolean filter and an array
 * of indices. The filter and indices determine which rows of the table are
 * active. A table acquires a filter after a selection is performed on it
 * and indices after a join is performed on it. A LazyTable can be thought
 * of as something between a materialized view and an non-materialized view,
 * since it does not materialize the active rows of the table, but it does
 * contain all the information necessary to materialize the active rows.
 */
class LazyTable {
 public:
  LazyTable();
  /**
   * Construct a LazyTable
   *
   * @param table a table
   * @param filter A Boolean ChunkedArray Datum
   * @param indices An INT64 Array Datum
   */
  LazyTable(DBTable::TablePtr table, arrow::Datum filter, arrow::Datum indices,
            arrow::Datum index_chunks);

  LazyTable(
      DBTable::TablePtr table, arrow::Datum filter, arrow::Datum indices,
      arrow::Datum index_chunks,
      std::shared_ptr<phmap::flat_hash_map<int64_t, std::vector<RecordID>>>
          hash_table);

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param i column index
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> MaterializeColumn(int i);

  std::shared_ptr<phmap::flat_hash_map<int64_t, std::vector<RecordID>>>
  hash_table() {
    return hash_table_;
  }

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param col_name column name
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> MaterializeColumn(std::string col_name);

  void MaterializeColumn(Task* ctx, std::string col_name, arrow::Datum& out);

  void MaterializeColumn(Task* ctx, int i, arrow::Datum& out);

  inline void set_materialized_column(
      int i, std::shared_ptr<arrow::ChunkedArray> col) {
    materialized_cols_[i] = std::move(col);
  }

  inline void set_hash_table(
      std::shared_ptr<phmap::flat_hash_map<int64_t, std::vector<RecordID>>>
          hash_table) {
    hash_table_ = hash_table;
  }

  DBTable::TablePtr table;
  arrow::Datum filter;   // filters are ChunkedArrays
  arrow::Datum indices;  // indices are Arrays
  arrow::Datum index_chunks;

 private:
  // Hash table for the right table in each join
  std::shared_ptr<phmap::flat_hash_map<int64_t, std::vector<RecordID>>>
      hash_table_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> materialized_cols_;
  std::unordered_map<int, std::shared_ptr<arrow::ChunkedArray>> filtered_cols_;
  Context context_;
};

}  // namespace hustle::operators

#endif  // HUSTLE_OPERATORRESULT_H
