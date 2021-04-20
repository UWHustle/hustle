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

#include "indexed_block.h"

#include "hustle_block.h"
#include "metadata_units/sma.h"

namespace hustle::storage {
IndexedBlock::~IndexedBlock() = default;
std::vector<HustleIndex *> IndexedBlock::GenerateIndicesForColumn(
    int column_id) {
  std::vector<HustleIndex *> out;
  std::vector<HustleIndex *> verify_buffer;
  // TODO: (Type Handle) Handle SMA for different data type.
  switch (get_column(column_id)->type()->id()) {
    case arrow::Type::UINT8:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::INT16:
    case arrow::Type::UINT32:
    case arrow::Type::INT32:
    case arrow::Type::UINT64:
    case arrow::Type::INT64:
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::DECIMAL:
      verify_buffer.push_back(new Sma(get_column(column_id)));
      break;
    default:
      break;
  }
  for (int i = 0; i < verify_buffer.size(); i++) {
    auto entry = verify_buffer[i];
    if (entry->GetStatus().ok()) {
      out.push_back(&entry[i]);
    } else {
      // incompatible type error usually
      if (THROW_ERROR_ON_NOT_OK_METADATA) {
        throw std::runtime_error(entry->GetStatus().message());
      }
    }
  }
  return out;
}

bool IndexedBlock::SearchMetadata(
    int column_id, const arrow::Datum &val_ptr,
    arrow::compute::CompareOperator compare_operator) {
  if (column_indices_valid[column_id]) {
    for (auto &block_metadata : column_indices_list[column_id]) {
      if (!block_metadata->Search(val_ptr, compare_operator)) {
        return false;
      }
    }
  }
  return true;
}

std::vector<arrow::Status> IndexedBlock::GetIndexStatusList(int column_id) {
  std::vector<arrow::Status> out;
  out.reserve(column_indices_list[column_id].size());
  for (int i = 0; i < column_indices_list[column_id].size(); i++) {
    out.push_back(column_indices_list[column_id][i]->GetStatus());
  }
  return out;
}

int IndexedBlock::InsertRecord(uint8_t *record, int32_t *byte_widths) {
  int out = BaseBlock::InsertRecord(record, byte_widths);
  ProcessInsertion(out);
  return out;
}
int IndexedBlock::InsertRecord(std::vector<std::string_view> record,
                               int32_t *byte_widths) {
  int out = BaseBlock::InsertRecord(record, byte_widths);
  ProcessInsertion(out);
  return out;
}
int IndexedBlock::InsertRecords(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int out = BaseBlock::InsertRecords(column_data);
  ProcessInsertion(out);
  return out;
}
int IndexedBlock::InsertRecords(
    std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
    std::shared_ptr<arrow::Array> valid_column,
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int out =
      BaseBlock::InsertRecords(block_map, row_map, valid_column, column_data);
  ProcessInsertion(out);
  return out;
}


}  // namespace hustle::storage