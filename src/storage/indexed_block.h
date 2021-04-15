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

#ifndef HUSTLE_SRC_STORAGE_INDEXED_BLOCK
#define HUSTLE_SRC_STORAGE_INDEXED_BLOCK

#include <arrow/api.h>

#include <iostream>
#include <map>

#include "base_block.h"
#include "base_table.h"
#include "hustle_block.h"
#include "hustle_index.h"
#include "utils/bit_utils.h"

#define THROW_ERROR_ON_NOT_OK_METADATA true

namespace hustle::storage {

class IndexedBlock : public BaseBlock {
 public:
  ~IndexedBlock() override = default;

  inline IndexedBlock(int id, const std::shared_ptr<arrow::Schema> &schema,
                      int capacity)
      : BaseBlock(id, schema, capacity),
        column_indices_list(get_num_cols(), std::vector<HustleIndex *>(0)),
        column_indices_valid(get_num_cols(), false) {}

  inline IndexedBlock(int id,
                      const std::shared_ptr<arrow::RecordBatch> &record_batch,
                      int capacity)
      : BaseBlock(id, record_batch, capacity),
        column_indices_list(get_num_cols(), std::vector<HustleIndex *>(0)),
        column_indices_valid(get_num_cols(), false) {
    BuildMetadata();
  }

  /**
   * Build metadata for a single column by column ID.
   *
   * @param column_id column id
   */
  inline void BuildMetadata(int column_id) {
    column_indices_list[column_id] = GenerateIndicesForColumn(column_id);
    column_indices_valid[column_id] = true;
  }

  /**
   * Build metadata for all columns.
   */
  inline void BuildMetadata() {
    for (int i = 0; i < num_cols; i++) {
      BuildMetadata(i);
    }
  }

  /**
   * Perform a metadata search for a given column.
   *
   * @param column_id column id
   * @param val_ptr value ptr
   * @param compare_operator compare operator
   * @return false if values matching the given predicate are guaranteed
   * to not be in the block, otherwise true
   */
  bool SearchMetadata(int column_id, const arrow::Datum &val_ptr,
                      arrow::compute::CompareOperator compare_operator);

  /**
   * Perform a metadata search for a given column.
   *
   * @param column_name column name
   * @param val_ptr value ptr
   * @param compare_operator compare operator
   * @return false if values matching the given predicate are guaranteed
   * to not be in the block, otherwise true
   */
  inline bool SearchMetadata(const std::string &column_name,
                             const arrow::Datum &val_ptr,
                             arrow::compute::CompareOperator compare_operator) {
    return SearchMetadata(schema->GetFieldIndex(column_name), val_ptr,
                          compare_operator);
  }

  /**
   * Returns each metadata's arrow:Status object for a given column
   *
   * @param column_id column id
   * @return vector of arrow:Status objects
   */
  std::vector<arrow::Status> GetIndexStatusList(int column_id);

  int InsertRecord(uint8_t *record, int32_t *byte_widths) override;

  int InsertRecord(std::vector<std::string_view> record,
                   int32_t *byte_widths) override;

  int InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

  int InsertRecords(
      std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
      std::shared_ptr<arrow::Array> valid_column,
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

 protected:
  /**
   * List of Metadata lists per column to check when queried.
   */
  std::vector<std::vector<HustleIndex *>> column_indices_list;

  /**
   * Valid state of metadata list per column.
   */
  std::vector<bool> column_indices_valid;

  /**
   * Generates a column's metadata.
   *
   * @param column_id column
   * @return generated metadata
   */
  std::vector<HustleIndex *> GenerateIndicesForColumn(int column_id);

  /**
   * Process insertions into index tracked columns.
   *
   * @param insertion_return
   */
  inline void ProcessInsertion(int insertion_return) {
    if (insertion_return != -1) {
      std::fill(column_indices_valid.begin(), column_indices_valid.end(),
                false);
    }
  }
};

}  // namespace hustle::storage
#endif  // HUSTLE_SRC_STORAGE_INDEXED_BLOCK