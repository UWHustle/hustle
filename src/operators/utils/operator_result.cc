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

#include "operators/utils/operator_result.h"

#include <iostream>
#include <utility>

#include "storage/table.h"
#include "storage/utils/util.h"

namespace hustle::operators {

OperatorResult::OperatorResult(std::vector<LazyTable::LazyTablePtr> lazy_tables) {
  lazy_tables_ = lazy_tables;
}

OperatorResult::OperatorResult() = default;

void OperatorResult::append(DBTable::TablePtr table) {
    LazyTable::LazyTablePtr lazy_table_ptr = std::make_shared<LazyTable>(table, arrow::Datum(), arrow::Datum(), arrow::Datum());
  lazy_tables_.insert(lazy_tables_.begin(), lazy_table_ptr);
}

void OperatorResult::set_materialized_col(
    DBTable::TablePtr table, int i, std::shared_ptr<arrow::ChunkedArray> col) {
  get_table(table)->set_materialized_column(i, col);
}

void OperatorResult::append(LazyTable::LazyTablePtr lazy_table) {
  lazy_tables_.insert(lazy_tables_.begin(), lazy_table);
}

void OperatorResult::append(const std::shared_ptr<OperatorResult>& result) {
  for (auto& lazy_table : result->lazy_tables_) {
    lazy_tables_.push_back(lazy_table);
  }
}

LazyTable::LazyTablePtr OperatorResult::get_table(int i) { return lazy_tables_[i]; }

LazyTable::LazyTablePtr OperatorResult::get_table(const DBTable::TablePtr& table) {
    LazyTable::LazyTablePtr result = nullptr;
  for (auto& lazy_table : lazy_tables_) {
    if (lazy_table->table == table) {
      result = lazy_table;
      break;
    }
  }
  return result;
}

DBTable::TablePtr OperatorResult::materialize(
    const std::vector<ColumnReference>& col_refs, bool metadata_enabled) {
  arrow::Status status;
  arrow::SchemaBuilder schema_builder;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> out_cols;

  for (auto& col_ref : col_refs) {
    auto table = col_ref.table;
    auto col_name = col_ref.col_name;

    // When we want to materialize a new aggregate table.
    if (table == nullptr) {
      table = get_table(0)->table;
    }
    status =
        schema_builder.AddField(table->get_schema()->GetFieldByName(col_name));
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    auto col = get_table(table)->MaterializeColumn(col_name);
    out_cols.push_back(col);
  }

  status = schema_builder.Finish().status();
  evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  auto out_schema = schema_builder.Finish().ValueOrDie();

  auto out_table = std::make_shared<DBTable>("out", out_schema, BLOCK_SIZE,
                                             metadata_enabled);

  out_table->InsertRecords(out_cols);
  if(metadata_enabled) {
    out_table->BuildMetadata();
  }
  return out_table;
}

}  // namespace hustle::operators