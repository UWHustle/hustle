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

#include "hustle_block.h"
namespace hustle::storage {

HustleBlock::~HustleBlock() = default;

void HustleBlock::print() const {}
void HustleBlock::out_block(void *pArg, sqlite3_callback callback) const {}
int HustleBlock::InsertRecord(uint8_t *record, int32_t *byte_widths) {
  return 0;
}
int HustleBlock::InsertRecord(std::vector<std::string_view> record,
                              int32_t *byte_widths) {
  return 0;
}
int HustleBlock::InsertRecords(
    std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
    std::shared_ptr<arrow::Array> valid_column,
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  return 0;
}
int InsertRecord(uint8_t *record, int32_t *byte_widths) {}
int InsertRecord(std::vector<std::string_view> record, int32_t *byte_widths) {}
int InsertRecords(std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {}

int InsertRecords(std::map<int, BlockInfo> &block_map,
                  std::map<int, int> &row_map,
                  std::shared_ptr<arrow::Array> valid_column,
                  std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  return 0;
}
void TruncateBuffers() {}
}  // namespace hustle::storage
