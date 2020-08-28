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

#include <string>

#include "scheduler/task.h"
#include "storage/block.h"
#include "storage/table.h"
#include "utils/arrow_compute_wrappers.h"

namespace hustle::operators {

struct ColumnReference {
  std::shared_ptr<Table> table;
  std::string col_name;
};

/**
 * A LazyTable associates a table pointer with a boolean filter and an array
 * of indices. The filter and indices determine which rows of the table are
 * active. A table acquires a filter after a selection is performed on it and
 * indices after a join is performed on it. A LazyTable can be thought of as
 * something between a materialized view and an non-materialized view, since
 * it does not materialize the active rows of the table, but it does contain
 * all the information necessary to materialize the active rows.
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
  LazyTable(std::shared_ptr<Table> table, arrow::Datum filter,
            arrow::Datum indices, arrow::Datum index_chunks);

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param i column index
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column(int i);

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param col_name column name
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column_by_name(std::string col_name);

  std::shared_ptr<Table> table;
  arrow::Datum filter;   // filters are ChunkedArrays
  arrow::Datum indices;  // indices are Arrays
  arrow::Datum index_chunks;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> materialized_cols_;
  std::unordered_map<int, std::shared_ptr<arrow::ChunkedArray>> filtered_cols_;

  void get_column_by_name(Task* ctx, std::string col_name, arrow::Datum& out);

  void get_column(Task* ctx, int i, arrow::Datum& out);

  void set_materialized_column(int i, std::shared_ptr<arrow::ChunkedArray> col);

 private:
  //    std::vector<std::shared_ptr<arrow::ChunkedArray>> materialized_cols_;
  //    std::vector<bool> materialized_cols_bitmap_;
  Context context_;
};

}  // namespace hustle::operators

#endif  // HUSTLE_OPERATORRESULT_H
