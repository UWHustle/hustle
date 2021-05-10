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
#include "storage/ma_block.h"
#include "storage/utils/util.h"
#include "type/type_helper.h"

namespace hustle::storage {

    int  DBTable::query_type = -1;

DBTable::DBTable(std::string name, const std::shared_ptr<arrow::Schema> &schema,
                 int block_capacity, bool enable_metadata)
    : table_name(std::move(name)),
      schema(schema),
      metadata_enabled(enable_metadata),
      block_counter(0),
      num_rows(0),
      block_capacity(block_capacity),
      block_row_offsets({}) {
  fixed_record_width = compute_fixed_record_width(schema);
  num_cols = schema->num_fields();
}

DBTable::DBTable(
    std::string name,
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
    int block_capacity, bool enable_metadata)
    : table_name(std::move(name)),
      metadata_enabled(enable_metadata),
      block_counter(0),
      num_rows(0),
      block_capacity(block_capacity),
      block_row_offsets({0}) {
  schema = std::move(record_batches[0]->schema());
  num_cols = schema->num_fields();
  // Must be called only after schema is set
  fixed_record_width = compute_fixed_record_width(schema);
  std::shared_ptr<Block> block;
  for (const auto &batch : record_batches) {
    if (enable_metadata) {
      block = std::make_shared<MetadataAttachedBlock>(block_counter, batch,
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

void DBTable::InsertRecords(arrow::ChunkedArrayVector col_arrays) {
  std::vector<std::shared_ptr<arrow::ArrayData>> block_data;
  for (int i = 0; i < col_arrays[0]->num_chunks(); i++) {
    for (auto &col : col_arrays) {
      block_data.push_back(col->chunk(i)->data());
    }
    this->InsertRecords(block_data);
    block_data.clear();
  }
}

void DBTable::InsertRecords(
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

int DBTable::get_record_size(int32_t *byte_widths) {
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

void DBTable::InsertRecordTable(uint32_t rowId, uint8_t *record,
                                int32_t *byte_widths) {
  auto container = hustle::profiler.getContainer();
  int query_type = DBTable::query_type;
  container->startEvent("insert_record_"+ std::to_string(query_type));
  block_map[rowId] = InsertRecord(record, byte_widths);
  container->endEvent("insert_record_"+ std::to_string(query_type));
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
  auto container = hustle::profiler.getContainer();
  int query_type = DBTable::query_type;
  container->startEvent("update_record_"+ std::to_string(query_type));
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

    auto data_type = schema->field(col_num)->type();

    auto handler = [&]<typename T>(T *) {
      if constexpr (has_ctype_member<T>::value) {
        using CType = ArrowGetCType<T>;
        block->UpdateColumnValue<CType>(col_num, row_num, record + offset,
                                        byte_widths[col_num]);
      } else if constexpr (isOneOf<T, arrow::StringType>::value) {
        this->DeleteRecordTable(row_id);
        this->InsertRecordTable(row_id, record, byte_widths);
      } else {
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
      }
    };

    type_switcher(data_type, handler);
  }
  container->endEvent("update_record_"+ std::to_string(query_type));
}

void DBTable::DeleteRecordTable(uint32_t row_id) {
  auto container = hustle::profiler.getContainer();
    int query_type = DBTable::query_type;
  auto block_map_it = block_map.find(row_id);
    std::cout << "BEFORE DELETE RECORD TABLE"  << std::endl;

    if (block_map_it == block_map.end()) {
    return;
  }
  std::cout << "DELETE RECORD TABLE"  << std::endl;
  container->startEvent("delete_record_"+ std::to_string(query_type));

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
  container->endEvent("delete_record_"+ std::to_string(query_type));
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

bool DBTable::GetMetadataOk() {
  if (metadata_enabled) {
    for (size_t block_idx = 0; block_idx < get_num_blocks(); block_idx++) {
      bool block_compatible = get_block(block_idx)->IsMetadataCompatible();
      if (block_compatible) {
        auto metadata_block = std::static_pointer_cast<MetadataAttachedBlock>(
            get_block(block_idx));
        for (int col_idx = 0; col_idx < metadata_block->get_num_cols();
             col_idx++) {
          auto status_list = metadata_block->GetMetadataStatusList(col_idx);
          for (int status_idx = 0; status_idx < status_list.size();
               status_idx++) {
            if (!metadata_block->GetMetadataStatusList(col_idx)[status_idx]
                     .ok()) {
              return false;
            }
          }
        }
      }
    }
  }
  return true;
}

void DBTable::BuildMetadata() {
  if (metadata_enabled) {
    for (int i = 0; i < get_num_blocks(); i++) {
      bool block_compatible = get_block(i)->IsMetadataCompatible();
      if (!block_compatible) {
        // ignore blocks that are not metadata compatible
      } else {
        auto metadata_block =
            std::static_pointer_cast<MetadataAttachedBlock>(get_block(i));
        metadata_block->BuildMetadata();
      }
    }
  }
}

std::vector<std::vector<arrow::Status>> DBTable::GetMetadataStatusList(
    int column_id) {
  std::vector<std::vector<arrow::Status>> out;
  if (metadata_enabled) {
    for (int i = 0; i < get_num_blocks(); i++) {
      bool block_compatible = get_block(i)->IsMetadataCompatible();
      if (!block_compatible) {
        std::vector<arrow::Status> empty_list;
        out.push_back(empty_list);
      } else {
        auto metadata_block =
            std::static_pointer_cast<MetadataAttachedBlock>(get_block(i));
        out.push_back(metadata_block->GetMetadataStatusList(column_id));
      }
    }
  }
  return out;
}
}  // namespace hustle::storage
