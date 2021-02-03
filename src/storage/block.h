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

#ifndef HUSTLE_OFFLINE_BLOCK_H
#define HUSTLE_OFFLINE_BLOCK_H

#include <arrow/api.h>

#include <iostream>
#include <map>

#include "utils/bit_utils.h"

#define BLOCK_SIZE (1 << 20)

/**
 * A Hustle Block is a wrapper for an Arrow RecordBatch. An Arrow RecordBatch
 * is a table-like structure containing a vector of equal-length Arrow Arrays
 * An Arrow Array is a contiguous sequence of values of the same type. Both
 * RecordBatch and Array are immutable. Data within an Array can be mutated by
 * accessing the Array's underlying ArrayData, the mutable container holding
 * the data buffer. ArrayData buffers are always padded to multiples of 64
 * bytes.
 *
 * Because RecordBatch and Array are immutable, modifying the underlying
 * ArrayData will not update internal variables the RecordBatch nor the Array
 * e.g. inserting an element into ArrayData and incrementing its length will
 * not update the length of the Array.
 *
 * Currently, blocks are initialized with 0 rows, and memory is allocated upon
 * insertion as needed.
 *
 * Currently, only BOOL, INT64, STRING, and FIXEDSIZEBINARY types are support
 * However, support for any fixed-width type can be added by introducing
 * additional case statements in each switch block.
 */

namespace hustle::storage {

struct BlockInfo {
  int32_t block_id;
  int32_t row_num;
};

class Block {
 public:
  //
  /**
   * Initialize an empty block.
   *
   * @param id Block ID
   * @param schema Block schema, excluding the valid column
   * @param capacity Maximum number of date bytes to be stored in the Block
   */
  Block(int id, const std::shared_ptr<arrow::Schema> &schema, int capacity);

  /**
   * Initialize a Block from a RecordBatch read in from a file. This will
   * eventually be removed. The constructor that uses a vector of ArrayData
   * should be used instead.
   *
   * @param id Block ID
   * @param record_batch RecordBatch read from a file
   * @param capacity Maximum number of date bytes to be stored in the Block
   */
  Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int capacity);

  /**
   * Get the Block's ID
   *
   * @return The Block's ID
   */
  inline int get_id() const { return id; }

  /**
   * Return the block's schema, including the valid column.
   *
   * @return the block's schema
   */
  inline std::shared_ptr<arrow::Schema> get_schema() { return schema; }

  /**
   * Get a column from the Block by index. The indexing of columns is defined
   * by the schema definition.
   *
   * @param column_index Index of the column to be returned.
   * @return A read-only pointer to the column.
   */
  inline std::shared_ptr<arrow::Array> get_column(int column_index) const {
    return arrow::MakeArray(columns[column_index]);
  }

  /**
   * Get all columns from the Block.
   *
   * @return all columns in the block
   */
  inline std::vector<std::shared_ptr<arrow::ArrayData>> get_columns() {
    return columns;
  }

  /**
   * Get a column from the Block by name. Column names are defined by the
   * schema definition.
   *
   * @param name Name of the column
   * @return A read-only pointer to the column.
   */
  inline std::shared_ptr<arrow::Array> get_column_by_name(
      const std::string &name) const {
    return arrow::MakeArray(columns[schema->GetFieldIndex(name)]);
  }

  /**
   * Get the valid-bit column from the Block.
   *
   * @return valid-bit column
   */
  inline std::shared_ptr<arrow::Array> get_valid_column() const {
    return arrow::MakeArray(valid);
  }

  /**
   * Return the valid bit of a particular row.
   *
   * @param row_index Row index
   * @return True if the given row is valid, false otherwise.
   */
  inline bool get_valid(unsigned int row_index) const {
    auto *data = valid->GetMutableValues<uint8_t>(1, 0);
    uint8_t byte = data[row_index / 8];
    return ((byte >> (row_index % 8u)) & 1u) == 1u;
  }

  /**
   * Return the valid bit of a given valid-bit column.
   *
   * @param validArray Input valid-bit column.
   * @param row_index Row index.
   * @return True if the given row is valid, false otherwise.
   */
  inline bool get_valid(const std::shared_ptr<arrow::ArrayData> validArray,
                        unsigned int row_index) const {
    auto *data = validArray->GetMutableValues<uint8_t>(1, 0);
    uint8_t byte = data[row_index / 8];
    return ((byte >> (row_index % 8u)) & 1u) == 1u;
  }

  //
  /**
   * Set the valid bit to val at row_index
   *
   * @param row_index Row index
   * @param val Value to set the valid bit at row_index
   */
  inline void set_valid(unsigned int row_index, bool val) {
    auto *data = valid->GetMutableValues<uint8_t>(1, 0);
    if (val) {
      data[row_index / 8] |= (1u << (row_index % 8u));
    } else {
      data[row_index / 8] &= ~(1u << (row_index % 8u));
    }
  }

  /**
   * Get the Block's RecordBatch
   *
   * @return A pointer to the Block's RecordBatch
   */
  inline std::shared_ptr<arrow::RecordBatch> get_records() {
    return arrow::RecordBatch::Make(schema, num_rows, columns);
  }

  /**
   * Get the number of bytes that can still be added to the Block without
   * exceeding its capacity.
   *
   * @return Number of bytes that can still be added to the Block
   */
  inline int get_bytes_left() { return capacity - num_bytes; }

  /**
   * Print the contents of the block delimited by tabs, including the valid
   * column.
   */
  void print();

  /**
   * Get the number of rows in the Block.
   *
   * @return Number of rows in the Block
   */
  inline int get_num_rows() const { return num_rows; }

  /**
   * Get the number of columns in the Block.
   *
   * @return number of columns
   */
  inline int get_num_cols() const { return num_cols; }

  /**
   * Get the width of all fixed width columns.
   *
   * @return width
   */
  inline int get_fixed_record_width() const { return fixed_record_width; }

  /**
   * Get the maximum capacity of the Block.
   *
   * @return block capacity
   */
  inline int get_capacity() const { return capacity; }

  /**
   * Get the row ID Mapping of the block.
   *
   * @return row ID map
   */
  std::map<int, int> &get_row_id_map() { return row_id_map; }

  /**
   * Insert a record into the Block.
   *
   * @param record Values to be inserted into each column. Values should be
   * listed in the same order as they appear in the Block's schema. Values
   * should not be separated by e.g. null characters.
   * @param byte_widths Byte width of each value to be inserted. Byte widths
   * should be listed in the same order as they appear in the Block's schema.
   * @return -1 if the insertion failed, otherwise the highest row index now
   * present in the block.
   */
  int InsertRecord(uint8_t *record, int32_t *byte_widths);

  /**
   * Insert a record into the block.
   *
   * @param record Values to be inserted into each column.
   * @param byte_widths Byte width of each value to be inserted.
   * @return -1 if the insertion failed, otherwise the highest row index now
   * present in the block.
   */
  int InsertRecord(std::vector<std::string_view> record, int32_t *byte_widths);

  /**
   * Insert one or more records into the Block as a vector of ArrayData.
   * This insertion method would be used to insert the results of a query,
   * since query results are returned as Arrays.
   *
   * @param column_data Values to be inserted into each column, including
   * the valid column. Columns should be listed in the same order as they
   * appear in the Block's schema. The length of column_data must match the
   * length of the Block's schema. All ArrayData must contain the same
   * number of elements.
   * @return -1 if the insertion failed, otherwise the highest row index now
   * present in the block.
   */
  int InsertRecords(std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  /**
   * Insert one or more records into the Block using BlockInfo from a different
   * block.
   *
   * @param block_map BlockInfo Map
   * @param row_map Row Map
   * @param valid_column Valid-bit column
   * @param column_data Vector of column datas.
   * @return -1 if the insertion failed, otherwise the highest row index now
   * present in the block.
   */
  int InsertRecords(std::map<int, BlockInfo> &block_map,
                    std::map<int, int> &row_map,
                    std::shared_ptr<arrow::Array> valid_column,
                    std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  /**
   * Update a value in a column.
   *
   * @tparam field_size
   * @param col_num column number
   * @param row_num row number
   * @param record_value value to update field with
   * @param byte_width width of field
   */
  template <typename field_size>
  inline void UpdateColumnValue(int col_num, int row_num, uint8_t *record_value,
                                int byte_width) {
    auto *dest = columns[col_num]->GetMutableValues<field_size>(1, row_num);
    uint8_t *value = (uint8_t *)calloc(sizeof(field_size), sizeof(uint8_t));
    std::memcpy(value, utils::reverse_bytes(record_value, byte_width),
                byte_width);
    std::memcpy(dest, value, sizeof(field_size));
    free(value);
  }

  /**
   * Truncate column buffers for all columns in the Block.
   */
  void TruncateBuffers();

 private:
  /**
   * Block ID.
   */
  int id;

  /**
   * Block Schema.
   */
  std::shared_ptr<arrow::Schema> schema;

  /**
   * Block valid bit column.
   */
  std::shared_ptr<arrow::ArrayData> valid;

  /**
   * Columns stored within this block.
   */
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;

  /**
   * Sizes of each stored column.
   */
  std::vector<int> column_sizes;

  /**
   * Size of each column field.
   */
  std::vector<int32_t> field_sizes_;

  /**
   * Row ID Mapping
   */
  std::map<int, int> row_id_map;

  /**
   * Total number of data bytes stored in the block, excluding the valid column
   * data.
   */
  int num_bytes;

  /**
   * Capacity of the block, initialized to BLOCK_SIZE.
   */
  int capacity;

  /**
   * Width of all fixed width columns.
   */
  int fixed_record_width;

  /**
   * Number of rows in the Block, including valid and invalid rows.
   */
  int num_rows;

  /**
   * Number of columns in the Block, excluding the valid column.
   */
  int num_cols;

  /**
   * Compute the number of bytes in the block. This function is only called
   * when a block is initialized from a RecordBatch, i.e. when we read in a
   * block from a file.
   */
  void ComputeByteSize();

  /**
   * Allocate data for each column during Block creation.
   *
   * @tparam field_size
   * @param type type of column
   * @param init_rows number of rows to initialize the column with
   * @return
   */
  template <typename field_size>
  std::shared_ptr<arrow::ArrayData> AllocateColumnData(
      std::shared_ptr<arrow::DataType> type, int init_rows);

  /**
   * Insert a value into a column.
   *
   * @tparam field_size
   * @param col_num column number
   * @param head scanner head
   * @param record_value value to insert
   * @param byte_width width of field
   */
  template <typename field_size>
  void InsertValue(int col_num, int &head, uint8_t *record_value,
                   int byte_width);

  /**
   * Insert values into a column
   *
   * @tparam field_size
   * @param col_num column number
   * @param offset offset
   * @param vals values to insert
   * @param num_vals number of values to insert
   */
  template <typename field_size>
  void InsertValues(int col_num, int offset,
                    std::shared_ptr<arrow::ArrayData> vals, int num_vals);

  /**
   * Insert value from CSV string
   *
   * @tparam field_size
   * @param col_num column number
   * @param head scanner head
   * @param record CSV record
   * @param byte_width width of value to insert
   */
  template <typename field_size>
  void InsertCsvValue(int col_num, int &head, std::string_view record);

  /**
   * Truncate column buffer
   *
   * @tparam field_size
   * @param col_num column number
   */
  template <typename field_size>
  void TruncateColumnBuffer(int col_num);
};
}  // namespace hustle::storage
#endif  // HUSTLE_OFFLINE_BLOCK_H
