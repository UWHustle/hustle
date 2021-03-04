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

#include "operators/utils/lazy_table.h"
#include <cassert>
#include <iostream>
#include <utility>

#include "operators/utils/operator_result.h"
#include "scheduler/threading/synchronization_lock.h"
#include "storage/util.h"

namespace hustle::operators {

LazyTable::LazyTable() {
  filter = arrow::Datum();
  indices = arrow::Datum();
  index_chunks = arrow::Datum();
  hash_table_ = nullptr;
}

LazyTable::LazyTable(DBTable::TablePtr table, arrow::Datum filter,
                     arrow::Datum indices, arrow::Datum index_chunks) {
  this->table = table;
  this->filter = filter;
  this->indices = indices;
  this->index_chunks = index_chunks;
  this->hash_table_ = nullptr;

  materialized_cols_.resize(table->get_num_cols());
  filtered_cols_.reserve(table->get_num_cols());
}

LazyTable::LazyTable(
    DBTable::TablePtr table, arrow::Datum filter, arrow::Datum indices,
    arrow::Datum index_chunks,
    std::shared_ptr<phmap::flat_hash_map<int64_t, RecordID>> hash_table)
    : LazyTable(table, filter, indices, index_chunks) {
  this->hash_table_ = hash_table;
}

std::shared_ptr<arrow::ChunkedArray> LazyTable::MaterializeColumn(
    std::string col_name) {
  return MaterializeColumn(table->get_schema()->GetFieldIndex(col_name));
}

std::shared_ptr<arrow::ChunkedArray> LazyTable::MaterializeColumn(int i) {
  arrow::Status status;
  if (materialized_cols_[i] != nullptr) {
    return materialized_cols_[i];
  }
  assert(((filter.kind() == arrow::Datum::NONE) || (indices.kind() == arrow::Datum::NONE)) &&
    "Any one of the filter or indices are allowed to materialize column and not both.");
  auto col = arrow::Datum(table->get_column(i));
  if (filter.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Filter(col, filter).Value(&col);
  } else if (indices.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Take(col, indices).Value(&col);
  }

  std::shared_ptr<arrow::ChunkedArray> out_col = col.chunked_array();
  materialized_cols_[i] = out_col;
  return col.chunked_array();
}

void LazyTable::MaterializeColumn(Task *ctx, std::string col_name,
                                  arrow::Datum &out) {
    MaterializeColumn(ctx, table->get_schema()->GetFieldIndex(col_name), out);
}

void LazyTable::MaterializeColumn(Task *ctx, int i, arrow::Datum &out) {

  if (materialized_cols_[i] != nullptr) {
    out.value = materialized_cols_[i];
      return;
  }
    out = table->get_column(i);
    if (indices.kind() != arrow::Datum::NONE) {
      context_.apply_indices(ctx, out, indices, index_chunks, out);
    }
    materialized_cols_[i] = out.chunked_array();

}

}  // namespace hustle::operators
