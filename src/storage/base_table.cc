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

#include "base_table.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>

#include "storage/base_block.h"
#include "storage/utils/util.h"
#include "type/type_helper.h"

namespace hustle::storage {

BaseTable::~BaseTable() {}

BaseTable::BaseTable(std::string in_name,
                     const std::shared_ptr<arrow::Schema> &in_schema,
                     int in_block_capacity) {
  // base class initialization
  table_name = std::move(in_name);
  schema = in_schema;
  block_counter = 0;
  num_rows = 0;
  block_capacity = in_block_capacity;
  block_row_offsets = {};
  //
  fixed_record_width = compute_fixed_record_width(schema);
  num_cols = schema->num_fields();
}

BaseTable::BaseTable(
    std::string in_name,
    const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches,
    int in_block_capacity) {
  // base class initialization
  table_name = std::move(in_name);
  block_counter = 0;
  num_rows = 0;
  block_capacity = in_block_capacity;
  block_row_offsets = {0};
  //
  schema = std::move(record_batches[0]->schema());
  num_cols = schema->num_fields();
  // Must be called only after schema is set
  fixed_record_width = compute_fixed_record_width(schema);
  std::shared_ptr<HustleBlock> block;
  for (const auto &batch : record_batches) {
    block = GenerateNewBlock(block_counter, batch, BLOCK_SIZE);
    blocks.emplace(block_counter, block);
    block_counter++;
    num_rows += batch->num_rows();
    block_row_offsets.push_back(num_rows);
  }
  if (blocks[blocks.size() - 1]->get_bytes_left() > fixed_record_width) {
    MarkBlockForInsert(blocks[blocks.size() - 1]);
    // The final block is not full, so do not include an offset for the next
    // block.
    block_row_offsets[block_row_offsets.size() - 1] = -1;
  }
}

void BaseTable::InsertBlocks(
    std::vector<std::shared_ptr<HustleBlock>> input_blocks) {
  if (input_blocks.empty()) {
    return;
  }
  for (auto &block : input_blocks) {
    blocks.emplace(block_counter, block);
    block_counter++;
    num_rows += block->get_num_rows();
    block_row_offsets.push_back(num_rows);
  }
  if (blocks[blocks.size() - 1]->get_bytes_left() > fixed_record_width) {
    MarkBlockForInsert(blocks[blocks.size() - 1]);
    // The final block is not full, so do not include an offset for the next new
    // block.
    block_row_offsets[block_row_offsets.size()] = -1;
  }
}

void BaseTable::InsertRecords(arrow::ChunkedArrayVector col_arrays) {
  std::vector<std::shared_ptr<arrow::ArrayData>> block_data;
  for (int i = 0; i < col_arrays[0]->num_chunks(); i++) {
    for (auto &col : col_arrays) {
      block_data.push_back(col->chunk(i)->data());
    }
    this->InsertRecords(block_data);
    block_data.clear();
  }
}

void BaseTable::InsertRecords(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int l = column_data[0]->length;
  int data_size = 0;
  auto block = GetBlockForInsert();
  int offset = 0;

  // TODO: (Optimize) Why don't we switch column type then loop over rows?
  for (int row = 0; row < l; row++) {
    int record_size = 0;
    for (int i = 0; i < num_cols; i++) {
      auto data_type = schema->field(i)->type();
      // TODO: (Optimize) Make this function inline.
      auto handler = [&]<typename T>(T *) {
        if constexpr (has_ctype_member<T>::value) {
          using CType = ArrowGetCType<T>;
          int byte_width = sizeof(CType);
          record_size += byte_width;
        } else if constexpr (isOneOf<T, arrow::StringType>::value) {
          auto *offsets = column_data[i]->GetValues<int32_t>(1, 0);
          record_size += offsets[row + 1] - offsets[row];
        } else if constexpr (isOneOf<T, arrow::FixedSizeBinaryType>::value) {
          int byte_width =
              schema->field(i)->type()->layout().FixedWidth(1).byte_width;
          record_size += byte_width;
        } else {
          throw std::logic_error(
              std::string("Cannot compute record width. Unsupported type: ") +
              schema->field(i)->type()->ToString());
        }
      };

      type_switcher(data_type, handler);
    }
    std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;
    if (data_size + record_size > block->get_bytes_left()) {
      for (int i = 0; i < column_data.size(); i++) {
        // Note that row is equal to the index of the first record we
        // cannot fit in the block.
        auto sliced_data = column_data[i]->Slice(offset, row - offset);
        sliced_column_data.push_back(sliced_data);
      }
      block->InsertRecords(sliced_column_data);
      //            sliced_column_data.clear(); // no need to clear; a new
      //            vector is declared in each loop.
      offset = row;
      data_size = 0;
      block = CreateBlock();
    }
    data_size += record_size;
  }
  std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;
  // Insert the last of the records
  for (int i = 0; i < column_data.size(); i++) {
    // Note that row is equal to the index of the first record we
    // cannot fit in the block.
    auto sliced_data = column_data[i]->Slice(offset, l - offset);
    sliced_column_data.push_back(sliced_data);
  }
  block->InsertRecords(sliced_column_data);
  //    sliced_column_data.clear();
  // no need to clear. We only use this vector once.
  if (block->get_bytes_left() > fixed_record_width) {
    insert_pool[block->get_id()] = block;
  }
  num_rows += l;
}

// Tuple is passed in as an array of bytes which must be parsed.
BlockInfo BaseTable::InsertRecord(uint8_t *record, int32_t *byte_widths) {
  std::shared_ptr<HustleBlock> block = GetBlockForInsert();
  int32_t record_size = this->get_record_size(byte_widths);
  if (block->get_bytes_left() < record_size) {
    block = CreateBlock();
  }
  int32_t row_num = block->InsertRecord(record, byte_widths);
  num_rows++;
  if (block->get_bytes_left() > fixed_record_width) {
    insert_pool[block->get_id()] = block;
  }
  return {block->get_id(), row_num};
}

void BaseTable::UpdateRecord(uint32_t row_id, int num_UpdateMetaInfo,
                             UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                             int32_t *byte_widths) {
  auto block_map_it = block_map.find(row_id);
  if (block_map_it == block_map.end()) {
    return;
  }
  BlockInfo block_info = block_map_it->second;
  int row_num = block_info.row_num;
  std::shared_ptr<HustleBlock> block = this->get_block(block_info.block_id);
  int offset = 0;
  int curr_offset_col = 0;
  for (int i = 0; i < num_UpdateMetaInfo; i++) {
    int col_num = updateMetaInfo[i].colNum;

    while (col_num > curr_offset_col) {
      offset += byte_widths[curr_offset_col];
      curr_offset_col++;
    }

    auto data_type = schema->field(col_num)->type();

    auto handler = [&]<typename T>(T *) {
      if constexpr (has_ctype_member<T>::value) {
        using CType = ArrowGetCType<T>;
        block->UpdateColumnValue<CType>(col_num, row_num, record + offset,
                                        byte_widths[col_num]);
      } else if constexpr (isOneOf<T, arrow::StringType>::value) {
        this->DeleteRecord(row_id);
        this->InsertRecord(row_id, record, byte_widths);
      } else {
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
      }
    };

    type_switcher(data_type, handler);
  }
}

void BaseTable::DeleteRecord(uint32_t row_id) {
  auto block_map_it = block_map.find(row_id);
  if (block_map_it == block_map.end()) {
    return;
  }
  BlockInfo block_info = block_map_it->second;
  std::shared_ptr<HustleBlock> block = this->get_block(block_info.block_id);
  block->set_valid(block_info.row_num, false);
  auto updatedBlock =
      GenerateNewBlock(block_info.block_id, schema, block->get_capacity());
  updatedBlock->InsertRecords(block_map, block->get_row_id_map(),
                              block->get_valid_column(), block->get_columns());
  blocks[block_info.block_id] = updatedBlock;
  num_rows--;
  if (insert_pool.find(block_info.block_id) != insert_pool.end()) {
    insert_pool[block_info.block_id] = updatedBlock;
  }
}

void BaseTable::InsertRecord(std::vector<std::string_view> values,
                             int32_t *byte_widths) {
  std::shared_ptr<HustleBlock> block = GetBlockForInsert();
  int32_t record_size = 0;
  // record size is incorrectly computed!
  for (int i = 0; i < num_cols; i++) {
    record_size += byte_widths[i];
  }
  if (block->get_bytes_left() < record_size) {
    block = CreateBlock();
  }
  block->InsertRecord(values, byte_widths);
  num_rows++;
  if (block->get_bytes_left() > fixed_record_width) {
    insert_pool[block->get_id()] = block;
  }
}


}  // namespace hustle::storage
