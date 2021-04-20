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

#include "base_block.h"
#include "hustle_block.h"
#include "hustle_table.h"
#include "storage/cmemlog.h"

namespace hustle::storage {

/**
 * A Table is collection of Blocks. The Table schema does not include the valid
 * column, i.e. the valid column of each Block is hidden from Table.
 */
class BaseTable : public HustleTable {
 public:
  ~BaseTable() override;

  /**
   * Construct an empty table with no blocks.
   *
   * @param name Table name
   * @param schema Table schema, excluding the valid column
   * @param block_capacity Block size
   */
  BaseTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
            int block_capacity);

  /**
   * Construct a table from a vector of RecordBatches read from a file.
   *
   * @param name Table name
   * @param record_batches Vector of RecordBatches read from a file
   * @param block_capacity Block size
   */
  BaseTable(
      std::string name,
      const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches,
      int block_capacity);

  void InsertBlocks(
      std::vector<std::shared_ptr<HustleBlock>> input_blocks) override;

  BlockInfo InsertRecord(uint8_t *record, int32_t *byte_widths) override;

  inline void InsertRecord(uint32_t rowId, uint8_t *record,
                           int32_t *byte_widths) override {
    block_map[rowId] = InsertRecord(record, byte_widths);
  }

  void InsertRecord(std::vector<std::string_view> values,
                    int32_t *byte_widths) override;

  void InsertRecords(arrow::ChunkedArrayVector col_arrays) override;

  void InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

  void UpdateRecord(uint32_t rowId, int nUpdateMetaInfo,
                    UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                    int32_t *byte_widths) override;

  void DeleteRecord(uint32_t rowId) override;

  inline void print() override {
    if (blocks.empty()) {
      std::cout << "Table is empty." << std::endl;
    } else {
      for (int i = 0; i < blocks.size(); i++) {
        blocks[i]->print();
      }
    }
  }

 protected:
  /**
   * Wrap block instantiation in an overridable member function.
   * If a table implementation uses a different kind of block, this is
   * where the replacement happens.
   *
   * @param block_id block_id
   * @param schema schema
   * @param capacity capacity
   * @return new block
   */
  virtual inline std::shared_ptr<HustleBlock> GenerateNewBlock(
      int block_id, const std::shared_ptr<arrow::Schema> &schema,
      int capacity) {
    return std::make_shared<BaseBlock>(block_id, schema, capacity);
  }

  /**
   * Wrap block instantiation in an overridable member function.
   * If a table implementation uses a different kind of block, this is
   * where the replacement happens.
   *
   * @param block_id block_id
   * @param batch record_batch
   * @param capacity capacity
   * @return new block
   */
  virtual inline std::shared_ptr<HustleBlock> GenerateNewBlock(
      int block_id, const std::shared_ptr<arrow::RecordBatch> &batch,
      int capacity) {
    return std::make_shared<BaseBlock>(block_id, batch, capacity);
  }

  /**
   * Create an empty Block to be added to the Table.
   *
   * @return An empty Block.
   */
  inline std::shared_ptr<HustleBlock> CreateBlock() {
    std::scoped_lock blocks_lock(blocks_mutex);
    int block_id = block_counter++;
    std::shared_ptr<HustleBlock> block;
    block = GenerateNewBlock(block_id, schema, block_capacity);
    blocks.emplace(block_id, block);
    block_row_offsets.push_back(num_rows);
    return block;
  }

  /**
   * @return Block from the insert_pool
   */
  inline std::shared_ptr<HustleBlock> GetBlockForInsert() {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    if (insert_pool.empty()) {
      return CreateBlock();
    }
    auto iter = insert_pool.begin();
    std::shared_ptr<HustleBlock> block = iter->second;
    insert_pool.erase(iter->first);
    return block;
  }

  /**
   * Mark a block to be added to the insert pool.
   *
   * @param block Block to be added to the insert pool.
   */
  inline void MarkBlockForInsert(const std::shared_ptr<HustleBlock> &block) {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    assert(block->get_bytes_left() > fixed_record_width);
    insert_pool[block->get_id()] = block;
  }

  /**
   * Mutex locked during block creation and insertion.
   */
  std::mutex blocks_mutex;

  /**
   * A map containing blocks that can hold at least one additional record.
   */
  std::unordered_map<int, std::shared_ptr<HustleBlock>> insert_pool;

  /**
   * Mutex locked when retrieving blocks from the insert pool.
   */
  std::mutex insert_pool_mutex;
};

}  // namespace hustle::storage
#endif  // HUSTLE_OFFLINE_TABLE_H
