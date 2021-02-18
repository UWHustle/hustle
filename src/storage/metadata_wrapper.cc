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

#include "metadata_wrapper.h"

namespace hustle::storage {

bool MetadataEnabledBlock::SearchMetadata(
    int column_id, const arrow::Datum &val_ptr,
    arrow::compute::CompareOperator compare_operator) {
  if (CheckValidMetadata(column_id)) {
    for (auto &block_metadata : column_metadata_list[column_id]) {
      if (block_metadata->IsCompatible(val_ptr.type())) {
        if (!block_metadata->Search(val_ptr, compare_operator)) {
          return false;
        }
      }
    }
  }
  return true;
}

std::vector<BlockMetadata *> MetadataEnabledBlock::GenerateMetadataForColumn(
    int column_id) {
  std::vector<BlockMetadata *> out;
  std::vector<BlockMetadata *> verify_buffer;
  switch (get_column(column_id)->type()->id()) {
    // case arrow::Type::NA:
    // case arrow::Type::BOOL:
    // case arrow::Type::STRING:
    // case arrow::Type::INTERVAL_MONTHS:
    // case arrow::Type::INTERVAL_DAY_TIME:
    // case arrow::Type::LIST:
    // case arrow::Type::STRUCT:
    // case arrow::Type::SPARSE_UNION:
    // case arrow::Type::DENSE_UNION:
    // case arrow::Type::DICTIONARY:
    // case arrow::Type::MAP:
    // case arrow::Type::EXTENSION:
    // case arrow::Type::FIXED_SIZE_LIST:
    // case arrow::Type::LARGE_STRING:
    // case arrow::Type::LARGE_LIST:
    // case arrow::Type::MAX_ID:
    default:
      break;
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
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
    case arrow::Type::TIMESTAMP:
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
    case arrow::Type::DECIMAL:
    case arrow::Type::DURATION:
    case arrow::Type::LARGE_BINARY:
      verify_buffer.push_back(new Sma(get_column(column_id)));
      break;
  }
  for (int i = 0; i < verify_buffer.size(); i++) {
    auto entry = verify_buffer[i];
    if (entry->IsOkay()) {
      out.push_back(&entry[i]);
    } else {
      throw std::runtime_error(entry->GetStatus().message());
    }
  }
  return out;
}

std::vector<arrow::Status> MetadataEnabledBlock::GetMetadataStatusList(
    int column_id) {
  std::vector<arrow::Status> out;
  out.reserve(column_metadata_list[column_id].size());
  for (int i = 0; i < column_metadata_list[column_id].size(); i++) {
    out.push_back(column_metadata_list[column_id][i]->GetStatus());
  }
  return out;
}
}  // namespace hustle::storage
