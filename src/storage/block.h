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
#include <map>
#include <iostream>
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
 *
 * TODO(nicholas): In the future, blocks will "guess" how many records they can
 * hold and allocate enough memory for that many records. If this guess is too
 * large/small, then we can allocate/deallocate memory accordingly.
 *
 * TODO(nicholas): It may be better to store a vector of mutable ArrayDatas
 * instead of a RecordBatch. We could mutate the ArrayData without having to
 * worry about internal inconsistencies. If we need to run a query on a block
 * we could construct a RecordBatch from the vector of ArrayData and return
 * that instead. If we need to fetch just one column, we could construct a
 * Array from the corresponding ArrayData. With this approach, we avoid
 * internal consistencies and without having to reconstruct RecordBatches
 */

namespace hustle::storage {

struct BlockInfo {
  int32_t blockId;
  int32_t rowNum;
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
   * Initialize a Block from a vector of ArrayData
   *
   * @param id Block ID
   * @param schema Block schema, excluding the valid column
   * @param column_data ArrayData for each column
   * @param capacity Maximum number of date bytes to be stored in the Block
   */
  Block(int id, const std::shared_ptr<arrow::Schema> &schema,
        std::vector<std::shared_ptr<arrow::ArrayData>> column_data,
        int capacity);

  /**
   * Get the Block's ID
   *
   * @return The Block's ID
   */
  int get_id() const;

  /**
   * Return the block's schema, including the valid column.
   * @return the block's schema
   */
  std::shared_ptr<arrow::Schema> get_schema();

  /**
   * Get a column from the Block by index. The indexing of columns is defined
   * by the schema definition.
   *
   * @param column_index Index of the column to be returned.
   * @return A read-only pointer to the column.
   */
  std::shared_ptr<arrow::Array> get_column(int column_index) const;

  /**
   *
   *
   * @return
   */
  std::shared_ptr<arrow::Array> get_valid_column() const;

  /**
   * Get a column from the Block by name. Column names are defined by the
   * schema definition.
   *
   * @param name Name of the column
   * @return A read-only pointer to the column.
   */
  std::shared_ptr<arrow::Array> get_column_by_name(
      const std::string &name) const;

  /**
   * Determine the first available row that can be used to store the data.
   * This function is not used right now, since memory is allocated upon
   * insertion as necessary.
   *
   * @return If a free row exists, return it's index. Otherwise, return -1.
   */
  int get_free_row_index() const;

  /**
   *  Return the valid bit of a particular row.
   *
   * @param row_index Row index
   * @return True is a the row at row_index contains valid data, false
   * otherwise.
   */
  bool get_valid(unsigned int row_index) const;

  bool get_valid(std::shared_ptr<arrow::ArrayData> valid, 
                                      unsigned int row_index) const;

  //
  /**
   * Set the valid bit to val at row_index
   *
   * @param row_index Row index
   * @param val Value to set the valid bit at row_index
   */
  void set_valid(unsigned int row_index, bool val);

  /**
   * Increment the number of rows stored in the Block. Note that this does NOT
   * increment the number of rows in the RecordBatch nor the length of its
   * Arrays.
   */
  void increment_num_rows();

  /**
   * Decrement the number of rows stored in the Block. Note that this does NOT
   * decrement the number of rows in the RecordBatch now the length of its
   * Arrays.
   */
  void decrement_num_rows();

  /**
   * Increment the number of bytes stored in the Block by bytes.
   *
   * @param bytes Number of bytes added to the Block.
   */
  void increment_num_bytes(unsigned int bytes);

  std::vector<std::shared_ptr<arrow::ArrayData>> get_columns() {
      return columns;
  }

  /**
   * Get the Block's RecordBatch
   *
   * @return A pointer to the Block's RecordBatch
   */
  std::shared_ptr<arrow::RecordBatch> get_records();

  /**
   * Get the number of bytes that can still be added to the Block without
   * exceeding its capacity.
   *
   * @return Number of bytes that can still be added to the Block
   */
  int get_bytes_left();

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
  int get_num_rows() const;

  int get_num_cols() const;

  int get_fixed_record_width() const {
      return fixed_record_width;
  }

  int get_capacity() {
      return capacity;
  }

  std::map<int, int>& get_row_id_map() {
      return row_id_map;
  }

  /**
   * Insert a record into the Block.
   *
   * @param record Values to be inserted into each column. Values should be
   * listed in the same order as they appear in the Block's schema. Values
   * should not be separated by e.g. null characters.
   * @param byte_widths Byte width of each value to be inserted. Byte widths
   * should be listed in the same order as they appear in the Block's schema.
   * @return True if insertion was successful, false otherwise.
   */
  int insert_record(uint8_t *record, int32_t *byte_widths);

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
   * @return True if insertion was successful, false otherwise.
   */
  bool insert_records(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  void insert_records(
      std::map<int, BlockInfo>& block_map,
      std::map<int, int>& row_map,
      std::shared_ptr<arrow::Array> valid_column,
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  bool insert_record(std::vector<std::string_view> record,
                     int32_t *byte_widths);

  bool insert_records(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data,
      int32_t offset, int64_t length);

  template <typename field_size>
  inline void update_value_in_column(int col_num, int row_num, uint8_t *record_value,
                                   int byte_width) {
    auto *dest = columns[col_num]->GetMutableValues<field_size>(1, row_num);
    uint8_t *value = (uint8_t *)calloc(sizeof(field_size), sizeof(uint8_t));
    std::memcpy(value, utils::reverse_bytes(record_value, byte_width),
                byte_width);
    std::memcpy(dest, value, sizeof(field_size));
    free(value);
  }

  void truncate_buffers();

 private:
  // Block ID
  int id;

  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::ArrayData> valid;
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  std::vector<int> column_sizes;

  std::vector<int32_t> field_sizes_;

  std::map<int, int> row_id_map;

  /**
   * Compute the number of bytes in the block. This function is only called
   * when a block is initialized from a RecordBatch, i.e. when we read in a
   * block from a file.
   *
   * TODO(nicholas): Instead of computing num_bytes, we can write num_bytes
   * at the beginning of each Block file and read it in before hand.
   */
  void compute_num_bytes();

  // Total number of data bytes stored in the block, excluding the valid
  // column data.
  int num_bytes;

  // Initialized to BLOCK_SIZE
  int capacity;

  int fixed_record_width;

  // Number of rows in the Block, including valid and invalid rows.
  int num_rows;

  // Number of columns in the Block, excluding the valid column.
  int num_cols;

  template <typename field_size>
  std::shared_ptr<arrow::ArrayData> allocate_column_data(
      std::shared_ptr<arrow::DataType> type, int init_rows);

  template <typename field_size>
  void insert_values_in_column(int i, int offset,
                               std::shared_ptr<arrow::ArrayData> vals,
                               int num_vals);

  template <typename field_size>
  void insert_value_in_column(int i, int &head, uint8_t *record_value,
                              int byte_width);

  template <typename field_size>
  void insert_csv_value_in_column(int i, int &head, std::string_view record,
                                  int byte_width);

  template <typename field_size>
  void truncate_column_buffer(int i);
};
}  // namespace hustle::storage
#endif  // HUSTLE_OFFLINE_BLOCK_H
