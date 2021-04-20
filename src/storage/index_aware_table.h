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

#ifndef HUSTLE_SRC_STORAGE_INDEX_AWARE_TABLE
#define HUSTLE_SRC_STORAGE_INDEX_AWARE_TABLE

#include <arrow/api.h>

#include <iostream>
#include <map>

#include "base_block.h"
#include "base_table.h"
#include "hustle_block.h"
#include "hustle_index.h"
#include "hustle_table.h"
#include "indexed_block.h"
#include "utils/bit_utils.h"

#define ENABLE_DIRTY_LIST false
#define ENABLE_INDEXING true

namespace hustle::storage {

class IndexAwareTable : public BaseTable {
 public:
  ~IndexAwareTable() override;

  inline IndexAwareTable(std::string name,
                         const std::shared_ptr<arrow::Schema> &schema,
                         int block_capacity)
      : BaseTable(std::move(name), schema, block_capacity),
        enable_dirty_list(ENABLE_DIRTY_LIST),
        allow_emptying_dirty_list(ENABLE_DIRTY_LIST),
        dirty_list({}) {}

  inline IndexAwareTable(
      std::string name,
      const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches,
      int block_capacity)
      : BaseTable(std::move(name), schema, block_capacity),
        enable_dirty_list(ENABLE_DIRTY_LIST),
        allow_emptying_dirty_list(ENABLE_DIRTY_LIST),
        dirty_list({}) {
    GenerateIndices();
  }

  void ReceiveSignal(HustleStorableSignal signal) override {
    BaseTable::ReceiveSignal(signal);
    if (ENABLE_DIRTY_LIST) {
      switch (signal) {
        case QUERY_BEGIN:
          allow_emptying_dirty_list = false;
          break;
        case QUERY_END:
          allow_emptying_dirty_list = true;
          ProcessDirtyList();
          break;
        default:
          break;
      }
    }
  }

  void InsertBlocks(
      std::vector<std::shared_ptr<HustleBlock>> input_blocks) override;

  BlockInfo InsertRecord(uint8_t *record, int32_t *byte_widths) override;

  void InsertRecord(uint32_t rowId, uint8_t *record,
                    int32_t *byte_widths) override;

  void InsertRecord(std::vector<std::string_view> values,
                    int32_t *byte_widths) override;

  void InsertRecords(arrow::ChunkedArrayVector col_arrays) override;

  void InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

  void UpdateRecord(uint32_t rowId, int nUpdateMetaInfo,
                    UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                    int32_t *byte_widths) override;

  void DeleteRecord(uint32_t rowId) override;

  /**
   * Get a vector of index statuses for a single column across
   * all blocks
   *
   * @param column_id column_id
   * @return vector of index statuses
   */
  std::vector<arrow::Status> GetIndexStatusList(int column_id) const;

  /**
   * Generate indices for all columns
   */
  void GenerateIndices();

 protected:
  inline std::shared_ptr<HustleBlock> GenerateNewBlock(
      int block_id, const std::shared_ptr<arrow::Schema> &schema,
      int capacity) override {
    return std::make_shared<IndexedBlock>(block_id, schema, capacity);
  }

  inline std::shared_ptr<HustleBlock> GenerateNewBlock(
      int block_id, const std::shared_ptr<arrow::RecordBatch> &batch,
      int capacity) override {
    return std::make_shared<IndexedBlock>(block_id, batch, capacity);
  }

  /**
   * Allow for an index to be rebuilt after
   */
  bool enable_dirty_list;

  bool allow_emptying_dirty_list;

  std::vector<std::shared_ptr<HustleBlock>> dirty_list;

  /**
   *
   * @param modified_row_id
   */
  void AddBlockToDirtyList(uint32_t modified_row_id);

  /**
   * Regenerate columns of blocks in the dirty list.
   */
  void ProcessDirtyList();
};

}  // namespace hustle::storage
#endif  // HUSTLE_SRC_STORAGE_INDEX_AWARE_TABLE