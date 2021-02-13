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
  auto sma_meta = new Sma(get_column(column_id));
  if (sma_meta->IsOkay()) {
    out.push_back(sma_meta);
  }
  return out;
}
}  // namespace hustle::storage
