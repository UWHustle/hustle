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

#include "index_aware_table.h"

#include "hustle_block.h"

namespace hustle::storage {

void IndexAwareTable::InsertBlocks(
    std::vector<std::shared_ptr<HustleBlock>> input_blocks) {
  BaseTable::InsertBlocks(input_blocks);
}

BlockInfo IndexAwareTable::InsertRecord(uint8_t *record, int32_t *byte_widths) {
  auto out = BaseTable::InsertRecord(record, byte_widths);
  return out;
}
void IndexAwareTable::InsertRecord(uint32_t rowId, uint8_t *record,
                                   int32_t *byte_widths) {
  BaseTable::InsertRecord(rowId, record, byte_widths);
}
void IndexAwareTable::InsertRecord(std::vector<std::string_view> values,
                                   int32_t *byte_widths) {
  BaseTable::InsertRecord(values, byte_widths);
}
void IndexAwareTable::InsertRecords(arrow::ChunkedArrayVector col_arrays) {
  BaseTable::InsertRecords(col_arrays);
}
void IndexAwareTable::InsertRecords(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  BaseTable::InsertRecords(column_data);
}
void IndexAwareTable::UpdateRecord(uint32_t rowId, int nUpdateMetaInfo,
                                   UpdateMetaInfo *updateMetaInfo,
                                   uint8_t *record, int32_t *byte_widths) {
  BaseTable::UpdateRecord(rowId, nUpdateMetaInfo, updateMetaInfo, record,
                          byte_widths);
}
void IndexAwareTable::DeleteRecord(uint32_t rowId) {
  BaseTable::DeleteRecord(rowId);
}
void IndexAwareTable::AddBlockToDirtyList(uint32_t modified_row_id) {
  if (enable_dirty_list) {
    // TODO
  }
}

std::vector<arrow::Status> IndexAwareTable::GetIndexStatusList(
    int column_id) const {
  std::vector<arrow::Status> out;
  for (int i = 0; i < blocks.size(); i++) {
    std::shared_ptr<IndexedBlock> casted_block =
        std::dynamic_pointer_cast<IndexedBlock>(blocks.at(i));
    if (casted_block != nullptr) {
      auto status_list = casted_block->GetIndexStatusList(column_id);
      for (int j = 0; j < status_list.size(); j++) {
        out.push_back(status_list.at(j));
      }
    }
  }
  return out;
}
void IndexAwareTable::ProcessDirtyList() {
  if (allow_emptying_dirty_list) {
    for (int i = 0; i < dirty_list.size(); i++) {
      std::shared_ptr<IndexedBlock> casted_block =
          std::dynamic_pointer_cast<IndexedBlock>(dirty_list.at(i));
      if (casted_block != nullptr) {
        casted_block->BuildMetadata();
      }
    }
  }
}
void IndexAwareTable::GenerateIndices() {
  if (ENABLE_INDEXING) {
    for (int i = 0; i < blocks.size(); i++) {
      std::shared_ptr<IndexedBlock> casted_block =
          std::dynamic_pointer_cast<IndexedBlock>(blocks.at(i));
      if (casted_block != nullptr) {
        casted_block->BuildMetadata();
      }
    }
  }
}

}  // namespace hustle::storage
