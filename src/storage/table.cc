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

#include "table.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>

#include "storage/block.h"
#include "storage/metadata_wrapper.h"
#include "storage/util.h"

#define ENABLE_METADATA_TABLE_FROM_RECORD_BATCH true

namespace hustle::storage {

DBTable::DBTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
                 int block_capacity)
    : table_name(std::move(name)),
      schema(schema),
      block_counter(0),
      num_rows(0),
      block_capacity(block_capacity),
      block_row_offsets({}) {
  //
  fixed_record_width = compute_fixed_record_width(schema);
  num_cols = schema->num_fields();
}

DBTable::DBTable(
    std::string name,
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
    int block_capacity)
    : table_name(std::move(name)),
      block_counter(0),
      num_rows(0),
      block_capacity(block_capacity),
      block_row_offsets({0}) {
  //
  schema = std::move(record_batches[0]->schema());
  num_cols = schema->num_fields();
  // Must be called only after schema is set
  fixed_record_width = compute_fixed_record_width(schema);
  std::shared_ptr<Block> block;
  for (const auto &batch : record_batches) {
    if (ENABLE_METADATA_TABLE_FROM_RECORD_BATCH) {
      block = std::make_shared<MetadataEnabledBlock>(block_counter, batch,
                                                     BLOCK_SIZE);
    } else {
      block = std::make_shared<Block>(block_counter, batch, BLOCK_SIZE);
    }
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

void DBTable::InsertBlocks(std::vector<std::shared_ptr<Block>> input_blocks) {
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

void DBTable::InsertRecords(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int l = column_data[0]->length;
  int data_size = 0;
  auto block = GetBlockForInsert();
  int offset = 0;
  int length = l;
  // optimize?
  int column_types[num_cols];
  for (int i = 0; i < num_cols; i++) {
    column_types[i] = schema->field(i)->type()->id();
  }
  for (int row = 0; row < l; row++) {
    int record_size = 0;
    for (int i = 0; i < num_cols; i++) {
      switch (column_types[i]) {
        case arrow::Type::STRING: {
          // optimize with offsets per schema?
          auto *offsets = column_data[i]->GetValues<int32_t>(1, 0);
          record_size += offsets[row + 1] - offsets[row];
          break;
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
          int byte_width =
              schema->field(i)->type()->layout().FixedWidth(1).byte_width;
          record_size += byte_width;
          break;
        }
        case arrow::Type::DOUBLE:
        case arrow::Type::INT64: {
          int byte_width = sizeof(int64_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT32: {
          int byte_width = sizeof(uint32_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT16: {
          int byte_width = sizeof(uint16_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT8: {
          int byte_width = sizeof(uint8_t);
          record_size += byte_width;
          break;
        }
        default: {
          throw std::logic_error(
              std::string("Cannot compute record width. Unsupported type: ") +
              schema->field(i)->type()->ToString());
        }
      }
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

int DBTable::get_record_size(int32_t *byte_widths) {
  int record_size = 0;
  int num_cols = get_num_cols();
  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        record_size += byte_widths[i];
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        record_size += sizeof(int64_t);
        break;
      }
      case arrow::Type::UINT32: {
        record_size += sizeof(uint32_t);
        break;
      }
      case arrow::Type::UINT16: {
        record_size += sizeof(uint16_t);
        break;
      }
      case arrow::Type::UINT8: {
        record_size += sizeof(uint8_t);
        break;
      }
      default:
        throw std::logic_error(std::string("unsupported type: ") +
                               schema->field(i)->type()->ToString());
    }
  }
  return record_size;
}

// Tuple is passed in as an array of bytes which must be parsed.
BlockInfo DBTable::InsertRecord(uint8_t *record, int32_t *byte_widths) {
  std::shared_ptr<Block> block = GetBlockForInsert();
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

void DBTable::UpdateRecordTable(uint32_t row_id, int num_UpdateMetaInfo,
                                UpdateMetaInfo *updateMetaInfo, uint8_t *record,
                                int32_t *byte_widths) {
  auto block_map_it = block_map.find(row_id);
  if (block_map_it == block_map.end()) {
    return;
  }
  BlockInfo block_info = block_map_it->second;
  int row_num = block_info.row_num;
  std::shared_ptr<Block> block = this->get_block(block_info.block_id);
  int offset = 0;
  int curr_offset_col = 0;
  for (int i = 0; i < num_UpdateMetaInfo; i++) {
    int col_num = updateMetaInfo[i].colNum;
    while (col_num > curr_offset_col) {
      offset += byte_widths[curr_offset_col];
      curr_offset_col++;
    }
    switch (schema->field(col_num)->type()->id()) {
      case arrow::Type::STRING: {
        this->DeleteRecordTable(row_id);
        this->InsertRecordTable(row_id, record, byte_widths);
        return;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        block->UpdateColumnValue<int64_t>(col_num, row_num, record + offset,
                                          byte_widths[col_num]);
        break;
      }
      case arrow::Type::UINT32: {
        block->UpdateColumnValue<uint32_t>(col_num, row_num, record + offset,
                                           byte_widths[i]);
        break;
      }
      case arrow::Type::UINT16: {
        block->UpdateColumnValue<uint32_t>(col_num, row_num, record + offset,
                                           byte_widths[i]);
        break;
      }
      case arrow::Type::UINT8: {
        block->UpdateColumnValue<uint8_t>(col_num, row_num, record + offset,
                                          byte_widths[i]);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
}

void DBTable::DeleteRecordTable(uint32_t row_id) {
  auto block_map_it = block_map.find(row_id);
  if (block_map_it == block_map.end()) {
    return;
  }
  BlockInfo block_info = block_map_it->second;
  std::shared_ptr<Block> block = this->get_block(block_info.block_id);
  block->set_valid(block_info.row_num, false);
  auto updatedBlock = std::make_shared<Block>(block_info.block_id, schema,
                                              block->get_capacity());
  updatedBlock->InsertRecords(block_map, block->get_row_id_map(),
                              block->get_valid_column(), block->get_columns());
  blocks[block_info.block_id] = updatedBlock;
  num_rows--;
  if (insert_pool.find(block_info.block_id) != insert_pool.end()) {
    insert_pool[block_info.block_id] = updatedBlock;
  }
}

void DBTable::InsertRecord(std::vector<std::string_view> values,
                           int32_t *byte_widths) {
  std::shared_ptr<Block> block = GetBlockForInsert();
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
