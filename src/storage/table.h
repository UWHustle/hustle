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

#ifndef HUSTLE_OFFLINE_TABLE_H
#define HUSTLE_OFFLINE_TABLE_H

#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "cmemlog.h"
#include "storage/block.h"

namespace hustle::storage {

/**
 * A Table is collection of Blocks. The Table schema does not include the valid
 * column, i.e. the valid column of each Block is hidden from Table.
 */
class DBTable {
 public:
  /**
   * Construct an empty table with no blocks.
   *
   * @param name Table name
   * @param schema Table schema, excluding the valid column
   * @param block_capacity Block size
   */
  DBTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
          int block_capacity);

  /**
   * Construct a table from a vector of RecordBatches read from a file.
   *
   * @param name Table name
   * @param record_batches Vector of RecordBatches read from a file
   * @param block_capacity Block size
   */
  DBTable(std::string name,
          std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
          int block_capacity);

  //
  /**
   * Create an empty Block to be added to the Table.
   *
   * @return An empty Block.
   */
  std::shared_ptr<Block> create_block();

  /**
   * Add a vector of blocks to the table. This functions does not check if
   * the blocks are consistent with the table or with each other. No memory
   * copying is done.
   *
   * @param intput_blocks Vector of blocks to be inserted into the table.
   */
  void insert_blocks(std::vector<std::shared_ptr<Block>> intput_blocks);

  /**
   * @param block_id Block ID
   * @return Block with the specified ID
   */
  std::shared_ptr<Block> get_block(int block_id) const;

  /**
   * @return Block from the insert_pool
   */
  std::shared_ptr<Block> get_block_for_insert();

  /**
   * @param block Block to be added to the insert pool
   */
  void mark_block_for_insert(const std::shared_ptr<Block> &block);

  int get_num_rows() const;

  size_t get_num_blocks() const;

  int get_record_size(int32_t *byte_widths);

  /**
   * Insert a record into a block in the insert pool.
   *
   * @param record Values to be inserted into each column. Values should be
   * listed in the same order as they appear in the Block's schema. Values
   * should not be separated by e.g. null characters.
   * @param byte_widths Byte width of each value to be inserted. Byte widths
   * should be listed in the same order as they appear in the Block's schema.
   */
  BlockInfo insert_record(uint8_t *record, int32_t *byte_widths);

  void insert_record_table(uint32_t rowId, uint8_t *record,
                           int32_t *byte_widths);

  void update_record_table(uint32_t rowId, int nUpdateMetaInfo,
                           UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                           int32_t *byte_widths);

  void delete_record_table(uint32_t rowId);
  /**
   * Insert one or more records into the Table as a vector of ArrayData.
   * This insertion method would be used to insert the results of a query,
   * since query results are returned as Arrays.
   *
   * @param column_data Values to be inserted into each column, including
   * the valid column. Columns should be listed in the same order as they
   * appear in the Table's schema. The length of column_data must match the
   * length of the Table's schema. All ArrayData must contain the same
   * number of elements.
   * @return True if insertion was successful, false otherwise.
   */
  void insert_records(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  /**
   * @return The Table's schema, excluding the valid column of the underlying
   * Blocks
   */
  std::shared_ptr<arrow::Schema> get_schema() const;

  /**
   * @return A map storing the Table's Blocks
   */
  std::unordered_map<int, std::shared_ptr<Block>> get_blocks();

  /**
   * Return the number of rows that appear before a specific block in the
   * table (the block's row offset)
   *
   * @param i Block index
   * @return The number of rows that appear before block i in the table.
   */
  int get_block_row_offset(int i) const;

  /**
   * Return a specific column as a ChunkedArray over all blocks in the table.
   *
   * @param i Column index
   * @return a ChunkedArray of column i over all blocks in the table.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column(int i);

  /**
   * Return a specific column as a ChunkedArray over all blocks in the table.
   *
   * @param name Column name
   * @return a ChunkedArray of column "name" over all blocks in the table.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column_by_name(
      const std::string &name);

  /**
   * Print the contents of all blocks in the table, including the valid
   * column.
   */
  void print();

  std::shared_ptr<arrow::ChunkedArray> get_valid_column();

  std::string get_name() const { return table_name; }

  int get_num_cols() const;

  void insert_record(std::vector<std::string_view> values,
                     int32_t *byte_widths);

  template <typename Functor>
  void ForEachBatch(const Functor &functor) const;

 private:
  std::string table_name;

  // Excludes the valid column, which is only visible to the Block class.
  std::shared_ptr<arrow::Schema> schema;

  // Initialized to BLOCK_SIZE
  int block_capacity;

  // row id to block info
  std::map<int, BlockInfo> block_map;

  std::unordered_map<int, std::shared_ptr<Block>> blocks;

  std::mutex blocks_mutex;

  // A map containing blocks that can hold at least one additional record.
  std::unordered_map<int, std::shared_ptr<Block>> insert_pool;

  std::mutex insert_pool_mutex;

  // Block ID of the next block to be created. Equal to the number of blocks
  // in the table.
  int block_counter;

  // The byte width of all fields with fixed length. If there are no
  // variable-length fields, then this is equal to the byte width of a full
  // record.
  //
  // Used to determine if we can fit another record into the block.
  int fixed_record_width;

  // Total number of rows in all blocks of the table.
  int num_rows;

  int num_cols;

  // The value at index i is the number of records before block i. The value
  // at index 0 is always 0.
  std::vector<int> block_row_offsets;
};

template <typename Functor>
void DBTable::ForEachBatch(const Functor &functor) const {
  size_t batch_size =
      this->get_num_blocks() / std::thread::hardware_concurrency();
  if (batch_size == 0) batch_size = this->get_num_blocks();
  size_t num_batches = 1 + ((this->get_num_blocks() - 1) / batch_size);
  for (size_t batch_index = 0; batch_index < num_batches; batch_index++) {
    functor(batch_index, batch_size);
  }
}

}  // namespace hustle::storage
#endif  // HUSTLE_OFFLINE_TABLE_H
