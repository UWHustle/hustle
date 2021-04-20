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

#include "hustle_block.h"
#include "utils/bit_utils.h"

namespace hustle::storage {

class BaseBlock : public HustleBlock {
 public:
  ~BaseBlock() override;

  /**
   * Initialize an empty block.
   *
   * @param id Block ID
   * @param schema Block schema, excluding the valid column
   * @param capacity Maximum number of date bytes to be stored in the Block
   */
  BaseBlock(int id, const std::shared_ptr<arrow::Schema> &schema, int capacity);

  /**
   * Initialize a Block from a RecordBatch read in from a file. This will
   * eventually be removed. The constructor that uses a vector of ArrayData
   * should be used instead.
   *
   * @param id Block ID
   * @param record_batch RecordBatch read from a file
   * @param capacity Maximum number of date bytes to be stored in the Block
   */
  BaseBlock(int id, const std::shared_ptr<arrow::RecordBatch> &record_batch,
            int capacity);

  void print() const override;

  void out_block(void *pArg, sqlite3_callback callback) const override;

  int InsertRecord(uint8_t *record, int32_t *byte_widths) override;

  int InsertRecord(std::vector<std::string_view> record,
                   int32_t *byte_widths) override;

  int InsertRecords(
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

  int InsertRecords(
      std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
      std::shared_ptr<arrow::Array> valid_column,
      std::vector<std::shared_ptr<arrow::ArrayData>> column_data) override;

  /**
   * Truncate column buffers for all columns in the Block.
   */
  void TruncateBuffers() override;

 protected:
  /**
   * Compute the number of bytes in the block. This function is only called
   * when a block is initialized from a RecordBatch.
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
