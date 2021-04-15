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

#include <storage/cmemlog.h>

#include <thread>

#include "hustle_block.h"
#include "hustle_storable.h"
#include "type/type_helper.h"

#ifndef HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_TABLE
#define HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_TABLE

namespace hustle::storage {

/**
 * A Table is collection of Blocks. The Table schema does not include the valid
 * column, i.e. the valid column of each Block is hidden from Table.
 */
class HustleTable : public HustleStorable {
 public:
  ~HustleTable() override = default;

  /**
   * Add a vector of blocks to the table. This functions does not check if
   * the blocks are consistent with the table or with each other. No memory
   * copying is done.
   *
   * @param input_blocks Vector of blocks to be inserted into the table.
   */
  virtual void InsertBlocks(
      std::vector<std::shared_ptr<HustleBlock>> input_blocks) = 0;

  /**
   * @param block_id Block ID
   * @return Block with the specified ID
   */
  inline std::shared_ptr<HustleBlock> get_block(int block_id) const {
    return blocks.at(block_id);
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
   * @param byte_widths
   * @return
   */
  inline int get_record_size(int32_t *byte_widths) const {
    int record_size = 0;
    int num_cols = get_num_cols();
    for (int i = 0; i < num_cols; i++) {
      auto data_type = schema->field(i)->type();
      auto handler = [&]<typename T>(T *) {
        if constexpr (has_ctype_member<T>::value) {
          record_size += sizeof(int64_t);
        } else if constexpr (isOneOf<T, arrow::StringType>::value) {
          record_size += byte_widths[i];
        } else {
          throw std::logic_error(std::string("unsupported type: ") +
                                 data_type->ToString());
        }
      };
      type_switcher(data_type, handler);
    }
    return record_size;
  }

  /**
   * Insert a record into a block in the insert pool.
   *
   * @param record Values to be inserted into each column. Values should be
   * listed in the same order as they appear in the Block's schema. Values
   * should not be separated by e.g. null characters.
   * @param byte_widths Byte width of each value to be inserted. Byte widths
   * should be listed in the same order as they appear in the Block's schema.
   */
  virtual BlockInfo InsertRecord(uint8_t *record, int32_t *byte_widths) = 0;

  /**
   * Insert record by row IDs.
   *
   * @param rowId row id
   * @param record record to insert
   * @param byte_widths widths of fields in record
   */
  virtual void InsertRecord(uint32_t rowId, uint8_t *record,
                            int32_t *byte_widths) = 0;

  /**
   * Insert a record into the table
   *
   * @param values values to be inserted
   * @param byte_widths width of each value
   */
  virtual void InsertRecord(std::vector<std::string_view> values,
                            int32_t *byte_widths) = 0;

  /**
   * Update record by row ID.
   *
   * @param rowId row id
   * @param nUpdateMetaInfo number of update meta infos
   * @param updateMetaInfo update meta info array
   * @param record updated record
   * @param byte_widths widths of fields in record
   */
  virtual void UpdateRecord(uint32_t rowId, int nUpdateMetaInfo,
                            UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                            int32_t *byte_widths) = 0;

  /**
   * Delete record by row id
   *
   * @param rowId row id
   */
  virtual void DeleteRecord(uint32_t rowId) = 0;

  /**
   *
   *
   * @param col_arrays
   */
  virtual void InsertRecords(arrow::ChunkedArrayVector col_arrays) = 0;

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
  virtual void InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) = 0;

  /**
   * @return The Table's schema, excluding the valid column of the underlying
   * Blocks
   */
  inline std::shared_ptr<arrow::Schema> get_schema() const { return schema; }

  /**
   * @return A map storing the Table's Blocks
   */
  inline std::unordered_map<int, std::shared_ptr<HustleBlock>> get_blocks()
      const {
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
  inline std::shared_ptr<arrow::ChunkedArray> get_column(
      int ColumnIndex) const {
    if (!blocks.empty()) {
      arrow::ArrayVector array_vector;
      for (int i = 0; i < blocks.size(); i++) {
        array_vector.push_back(blocks.at(i)->get_column(ColumnIndex));
      }
      return std::make_shared<arrow::ChunkedArray>(array_vector);
    } else {
      return nullptr;
    }
  }

  /**
   * Return a specific column as a ChunkedArray over all blocks in the table.
   *
   * @param name Column name
   * @return a ChunkedArray of column "name" over all blocks in the table.
   */
  inline std::shared_ptr<arrow::ChunkedArray> get_column_by_name(
      const std::string &name) const {
    return get_column(schema->GetFieldIndex(name));
  }

  /**
   * Print the contents of all blocks in the table, including the valid column.
   */
  virtual void print() = 0;

  /**
   * Outputs the table contents to the callback fn passed.
   * @param pArg call back arg to return the result from it
   * @param callback  output callback format (sqlite3 compatible)
   */
  inline void out_table(void *pArgs, sqlite3_callback callback) const {
    for (const auto &[blk_idx, blk] : blocks) {
      blk->out_block(pArgs, callback);
    }
  }

  /**
   * Get valid-bit column of all blocks in the table.
   *
   * @return valid-bit column
   */
  inline std::shared_ptr<arrow::ChunkedArray> get_valid_column() const {
    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
      array_vector.push_back(blocks.at(i)->get_valid_column());
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
   * Split the table into a number of batches and apply a function.
   *
   * @tparam Functor type
   * @param functor
   */
  template <typename Functor>
  inline void ForEachBatch(const Functor &functor) const {
    // if (this->get_num_blocks() == 0) return;
    size_t batch_size =
        this->get_num_blocks() / std::thread::hardware_concurrency();
    if (batch_size == 0) batch_size = this->get_num_blocks();
    size_t num_batches = 1 + ((this->get_num_blocks() - 1) / batch_size);
    for (size_t batch_index = 0; batch_index < num_batches; batch_index++) {
      functor(batch_index, batch_size);
    }
  }

 protected:
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
  std::unordered_map<int, std::shared_ptr<HustleBlock>> blocks;

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

}  // namespace hustle::storage
#endif  // HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_TABLE