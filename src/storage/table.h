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
#include "storage/ma_block.h"

#define ENABLE_METADATA_BY_DEFAULT true

namespace hustle::storage {

/**
 * A Table is collection of Blocks. The Table schema does not include the valid
 * column, i.e. the valid column of each Block is hidden from Table.
 */
class DBTable {
 public:
  using TablePtr = std::shared_ptr<DBTable>;
  /**
   * Construct an empty table with no blocks.
   *
   * @param name Table name
   * @param schema Table schema, excluding the valid column
   * @param block_capacity Block size
   * @param metadata_enabled true if the table should be metadata enabled, false
   * otherwise
   */
  DBTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
          int block_capacity, bool metadata_enabled);

  /**
   * Construct an empty table with no blocks.
   *
   * @param name Table name
   * @param schema Table schema, excluding the valid column
   * @param block_capacity Block size
   */
  inline DBTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
                 int block_capacity)
      : DBTable(name, schema, block_capacity, ENABLE_METADATA_BY_DEFAULT) {}

  /**
   * Construct a table from a vector of RecordBatches read from a file.
   *
   * @param name Table name
   * @param record_batches Vector of RecordBatches read from a file
   * @param block_capacity Block size
   * @param enable_metadata true if the table should be metadata enabled, false
   * otherwise
   */
  DBTable(std::string name,
          std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
          int block_capacity, bool enable_metadata);

  /**
   * Construct a table from a vector of RecordBatches read from a file.
   *
   * @param name Table name
   * @param record_batches Vector of RecordBatches read from a file
   * @param block_capacity Block size
   */
  inline DBTable(
      std::string name,
      std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
      int block_capacity)
      : DBTable(name, record_batches, block_capacity,
                ENABLE_METADATA_BY_DEFAULT) {}

  /**
   * Create an empty Block to be added to the Table.
   *
   * @return An empty Block.
   */
  inline std::shared_ptr<Block> CreateBlock() {
    std::scoped_lock blocks_lock(blocks_mutex);
    int block_id = block_counter++;
    std::shared_ptr<Block> block;
    if (metadata_enabled) {
      block = std::make_shared<MetadataAttachedBlock>(block_id, schema,
                                                     block_capacity);
    } else {
      block = std::make_shared<Block>(block_id, schema, block_capacity);
    }
    blocks.emplace(block_id, block);
    block_row_offsets.push_back(num_rows);
    return block;
  }

  /**
   * Add a vector of blocks to the table. This functions does not check if
   * the blocks are consistent with the table or with each other. No memory
   * copying is done.
   *
   * @param input_blocks Vector of blocks to be inserted into the table.
   */
  void InsertBlocks(std::vector<std::shared_ptr<Block>> input_blocks);

  /**
   * @param block_id Block ID
   * @return Block with the specified ID
   */
  inline std::shared_ptr<Block> get_block(int block_id) const {
    return blocks.at(block_id);
  }

  /**
   * @return Block from the insert_pool
   */
  inline std::shared_ptr<Block> GetBlockForInsert() {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    if (insert_pool.empty()) {
      return CreateBlock();
    }
    auto iter = insert_pool.begin();
    std::shared_ptr<Block> block = iter->second;
    insert_pool.erase(iter->first);
    return block;
  }

  /**
   * Mark a block to be added to the insert pool.
   *
   * @param block Block to be added to the insert pool.
   */
  inline void MarkBlockForInsert(const std::shared_ptr<Block> &block) {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    assert(block->get_bytes_left() > fixed_record_width);
    insert_pool[block->get_id()] = block;
  }

  /**
   * Get the number of rows in all blocks in the table.
   *
   * @return number of rows
   */
  inline int get_num_rows() const { return num_rows; }

  /**
   * Get the number of blocks in the table.
   *
   * @return number of blocks
   */
  inline size_t get_num_blocks() const { return blocks.size(); }

  /**
   * Get the size of one record in the Table.
   *
   * @param serial_types
   * @return
   */
  int get_record_size(int32_t *serial_types);

  /**
   * Insert a record into a block in the insert pool.
   *
   * @param record Values to be inserted into each column. Values should be
   * listed in the same order as they appear in the Block's schema. Values
   * should not be separated by e.g. null characters.
   * @param byte_widths Byte width of each value to be inserted. Byte widths
   * should be listed in the same order as they appear in the Block's schema.
   */
  BlockInfo InsertRecord(uint8_t *record, int32_t *serial_types);

  /**
   * Insert record by row IDs.
   *
   * @param rowId row id
   * @param record record to insert
   * @param byte_widths widths of fields in record
   */
  inline void InsertRecordTable(uint32_t rowId, uint8_t *record,
                                int32_t *serial_types) {
    block_map[rowId] = InsertRecord(record, serial_types);
  }

  /**
   * Update record by row ID.
   *
   * @param rowId row id
   * @param nUpdateMetaInfo number of update meta infos
   * @param updateMetaInfo update meta info array
   * @param record updated record
   * @param byte_widths widths of fields in record
   */
  void UpdateRecordTable(uint32_t rowId, int nUpdateMetaInfo,
                         UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                         int32_t *serial_types);

  /**
   * Delete record by row id
   *
   * @param rowId row id
   */
  void DeleteRecordTable(uint32_t rowId);

  void InsertRecords(arrow::ChunkedArrayVector col_arrays);
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
  void InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data);

  /**
   * @return The Table's schema, excluding the valid column of the underlying
   * Blocks
   */
  inline std::shared_ptr<arrow::Schema> get_schema() const { return schema; }

  /**
   * @return A map storing the Table's Blocks
   */
  inline std::unordered_map<int, std::shared_ptr<Block>> get_blocks() {
    return blocks;
  }

  /**
   * Return the number of rows that appear before a specific block in the
   * table (the block's row offset)
   *
   * @param i Block index
   * @return The number of rows that appear before block i in the table.
   */
  inline int get_block_row_offset(int block_index) const {
    return block_row_offsets[block_index];
  }

  /**
   * Return a specific column as a ChunkedArray over all blocks in the table.
   *
   * @param i Column index
   * @return a ChunkedArray of column i over all blocks in the table.
   */
  inline std::shared_ptr<arrow::ChunkedArray> get_column(int ColumnIndex) {
    if (blocks.empty()) {
      return nullptr;
    }
    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
      array_vector.push_back(blocks[i]->get_column(ColumnIndex));
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
  }

  /**
   * Return a specific column as a ChunkedArray over all blocks in the table.
   *
   * @param name Column name
   * @return a ChunkedArray of column "name" over all blocks in the table.
   */
  inline std::shared_ptr<arrow::ChunkedArray> get_column_by_name(
      const std::string &name) {
    return get_column(schema->GetFieldIndex(name));
  }

  /**
   * Print the contents of all blocks in the table, including the valid column.
   */
  inline void print() {
    if (blocks.empty()) {
      std::cout << "Table is empty." << std::endl;
    } else {
      for (int i = 0; i < blocks.size(); i++) {
        blocks[i]->print();
      }
    }
  }

  /**
   * Get valid-bit column of all blocks in the table.
   *
   * @return valid-bit column
   */
  inline std::shared_ptr<arrow::ChunkedArray> get_valid_column() {
    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
      array_vector.push_back(blocks[i]->get_valid_column());
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
  }

  /**
   * Get the name of the table
   *
   * @return table name
   */
  inline std::string get_name() const { return table_name; }

  /**
   * Get the number of columns in the table
   *
   * @return number of columns
   */
  inline int get_num_cols() const { return num_cols; }

  /**
   * Insert a record into the table
   *
   * @param values values to be inserted
   * @param byte_widths width of each value
   */
  void InsertRecord(std::vector<std::string_view> values, int32_t *serial_types);

  /**
   * Split the table into a number of batches and apply a function.
   *
   * @tparam Functor
   * @param functor
   */
  template <typename Functor>
  void ForEachBatch(const Functor &functor) const;

  /**
   * Get if the table generates metadata enabled blocks
   *
   * @return true if the table uses metadata enabled blocks, false otherwise
   */
  inline bool IsMetadataEnabled() { return metadata_enabled; }

  /**
   * Returns the status of all metadatas for a column, as individual lists per
   * block
   *
   * @return list of lists of metadata
   */
  std::vector<std::vector<arrow::Status>> GetMetadataStatusList(int column_id);

  /**
   * true if table is either not metadata enabled, or if the contained blocks'
   * metadata are all arrow::Status::Ok()
   *
   * @return true if metadata is ok
   */
  bool GetMetadataOk();

  /**
   * Build metadata for each block in the table if the block is
   * metadata compatible
   */
  void BuildMetadata();

 private:
  /**
   * Metadata enabled for table
   */
  const bool metadata_enabled;

  /**
   * Table Name
   */
  std::string table_name;

  /**
   * Table Schema. Note that this excludes the valid column, which is only
   * visible to the Block class.
   */
  std::shared_ptr<arrow::Schema> schema;

  /**
   * Capacity of each Block in the table. This is initialized to BLOCK_SIZE.
   */
  int block_capacity;

  /**
   * Block Map. This stores row id to block info.
   */
  std::map<int, BlockInfo> block_map;

  /**
   * Blocks stored in the table.
   */
  std::unordered_map<int, std::shared_ptr<Block>> blocks;

  /**
   * Mutex locked during block creation.
   */
  std::mutex blocks_mutex;

  /**
   * A map containing blocks that can hold at least one additional record.
   */
  std::unordered_map<int, std::shared_ptr<Block>> insert_pool;

  /**
   * Mutex locked when retrieving blocks from the insert pool.
   */
  std::mutex insert_pool_mutex;

  /**
   * Block ID of the next block to be created. Equal to the number of blocks in
   * the table.
   */
  int block_counter;

  /**
   * The byte width of all fields with fixed length. If there are no
   * variable-length fields, then this is equal to the byte width of a full
   * record. Used to determine if we can fit another record into the block.
   */
  int fixed_record_width;

  /**
   * Total number of rows in all blocks in the table.
   */
  int num_rows;

  /**
   * Total number of columns in all blocks in the table.
   */
  int num_cols;

  /**
   * The value at index i is the number of records before block i. The value at
   * index 0 is always 0.
   */
  std::vector<int> block_row_offsets;
};

/// Implementation of ForEachBatch
template <typename Functor>
void DBTable::ForEachBatch(const Functor &functor) const {
  // if (this->get_num_blocks() == 0) return;
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
