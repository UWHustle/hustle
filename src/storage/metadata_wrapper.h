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

#ifndef HUSTLE_METADATA_WRAPPER_H
#define HUSTLE_METADATA_WRAPPER_H

#include <arrow/compute/api.h>
#include <arrow/type.h>

#include "storage/block.h"
#include "storage/block_metadata.h"
#include "storage/block_metadata_impl/sma.h"

namespace hustle::storage {

class MetadataEnabledBlock : public Block {
 public:

  /**
   * Create a new wrapped, empty Block.
   *
   * @param id Block ID
   * @param schema Block schema, excluding the valid column
   * @param capacity Maximum number of data bytes to be stored in the Block
   */
  inline MetadataEnabledBlock(int id,
                              const std::shared_ptr<arrow::Schema> &schema,
                              int capacity)
      : Block(id, schema, capacity, true),
        column_metadata_list(get_num_cols(), std::vector<BlockMetadata *>(0)),
        column_metadata_valid(get_num_cols(), false) {
    /*
     * empty constructor
     * for now, we only generate metadata if a block is constructed with
     * a record batch.
     * Note that BuildMetadata() can be called at a later time.
     */
  }

  /**
   * Initialize a metadata wrapped block with data.
   *
   * @param id Block ID
   * @param record_batch record batch to initialize the block with
   * @param capacity Maximum number of data bytes to be stored in the block
   */
  inline MetadataEnabledBlock(int id,
                              std::shared_ptr<arrow::RecordBatch> record_batch,
                              int capacity)
      : Block(id, record_batch, capacity, true),
        column_metadata_list(get_num_cols(), std::vector<BlockMetadata *>(0)),
        column_metadata_valid(get_num_cols(), false) {
    BuildMetadata();
  }

  /**
   * Build metadata for a single column by column ID.
   *
   * @param column_id column id
   */
  inline void BuildMetadata(int column_id) {
    column_metadata_list[column_id] = GenerateMetadataForColumn(column_id);
    column_metadata_valid[column_id] = true;
  }

  /**
   * Build metadata for all columns.
   */
  inline void BuildMetadata() {
    for (int i = 0; i < get_num_cols(); i++) {
      BuildMetadata(i);
    }
  }

  /**
   * Build metadata for a single column by column name.
   *
   * @param name column name
   */
  inline void BuildMetadata(const std::string &name) {
    BuildMetadata(get_schema()->GetFieldIndex(name));
  }

  /**
   * Perform a metadata search for a given column.
   *
   * @param column_id column id
   * @param val_ptr value ptr
   * @param compare_operator
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
   * @param compare_operator arrow comparison operator
   * @return false if values matching the given predicate are guaranteed
   * to not be in the block, otherwise true
   */
  inline bool SearchMetadata(const std::string &column_name,
                             const arrow::Datum &val_ptr,
                             arrow::compute::CompareOperator compare_operator) {
    return SearchMetadata(get_schema()->GetFieldIndex(column_name), val_ptr,
                          compare_operator);
  }

  /**
   * Returns each metadata's arrow:Status object for a given column
   *
   * @param column_id column id
   * @return vector of arrow:Status objects
   */
  std::vector<arrow::Status> GetMetadataStatusList(int column_id);

  /**
   * Returns each metadata's arrow:Status object for a given column
   *
   * @param column_name column name
   * @return vector of arrow:Status objects
   */
  inline std::vector<arrow::Status> GetMetadataStatusList(const std::string &column_name) {
    return GetMetadataStatusList(get_schema()->GetFieldIndex(column_name));
  }

  // override
  using Block::InsertRecord;
  using Block::InsertRecords;
  using Block::UpdateColumnValue;

  /// override InsertRecord
  inline int InsertRecord(uint8_t *record, int32_t *byte_widths) {
    return WrapInsertion(Block::InsertRecord(record, byte_widths));
  }

  /// override InsertRecord
  inline int InsertRecord(std::vector<std::string_view> record,
                          int32_t *byte_widths) {
    return WrapInsertion(Block::InsertRecord(record, byte_widths));
  }

  /// override InsertRecords
  inline int InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
    return WrapInsertion(Block::InsertRecords(column_data));
  }

  /// override InsertRecords
  inline int InsertRecords(
      std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
      std::shared_ptr<arrow::Array> valid_column,
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
    return WrapInsertion(
        Block::InsertRecords(block_map, row_map, valid_column, column_data));
  }

  /// override UpdateColumnValue
  template <typename field_size>
  inline void UpdateColumnValue(int col_num, int row_num, uint8_t *record_value,
                                int byte_width) {
    Block::UpdateColumnValue<field_size>(col_num, row_num, record_value,
                                         byte_width);
    InvalidateMetadata(col_num);
  }

 private:

  /**
   * List of Metadata lists per column to check when queried.
   */
  std::vector<std::vector<BlockMetadata *>> column_metadata_list;

  /**
   * Valid state of metadata list per column.
   */
  std::vector<bool> column_metadata_valid;

  /**
   * Generates a column's metadata.
   *
   * @param column_id column
   * @return generated metadata
   */
  std::vector<BlockMetadata *> GenerateMetadataForColumn(int column_id);

  /**
   * Checks if the metadata for a column is valid.
   */
  inline bool CheckValidMetadata(int column_id) {
    return column_metadata_valid[column_id];
  }

  /**
   * Invalidate a column's metadata
   *
   * @param column_id column id
   */
  inline void InvalidateMetadata(int column_id) {
    column_metadata_valid[column_id] = false;
  }

  /**
   * Invalidate all column's metadata
   */
  inline void InvalidateMetadata() {
    std::fill(column_metadata_valid.begin(), column_metadata_valid.end(),
              false);
  }

  /**
   * Wrap insertion in metadata updating logic.
   *
   * @param insertion_return
   * @return the value of the original insertion
   */
  inline int WrapInsertion(int insertion_return) {
    if (insertion_return != -1) {
      InvalidateMetadata();
    }
    return insertion_return;
  }
};

}  // namespace hustle::storage
#endif  // HUSTLE_METADATA_WRAPPER_H
