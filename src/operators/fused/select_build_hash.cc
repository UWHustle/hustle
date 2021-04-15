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

#include "operators/fused/select_build_hash.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <utility>

#include "storage/base_table.h"
#include "storage/utils/util.h"
#include "utils/bit_utils.h"

namespace hustle {
namespace operators {

SelectBuildHash::SelectBuildHash(const std::size_t query_id,
                                 std::shared_ptr<HustleTable> table,
                                 OperatorResult::OpResultPtr prev_result,
                                 OperatorResult::OpResultPtr output_result,
                                 std::shared_ptr<PredicateTree> tree,
                                 ColumnReference join_column)
    : SelectBuildHash(query_id, table, prev_result, output_result, tree,
                      join_column, std::make_shared<OperatorOptions>()) {}

SelectBuildHash::SelectBuildHash(const std::size_t query_id,
                                 std::shared_ptr<HustleTable> table,
                                 OperatorResult::OpResultPtr prev_result,
                                 OperatorResult::OpResultPtr output_result,
                                 std::shared_ptr<PredicateTree> tree,
                                 ColumnReference join_column,
                                 std::shared_ptr<OperatorOptions> options)
    : Select(query_id, table, prev_result, output_result, tree, options),
      join_column_(join_column) {
  filters_.resize(table_->get_num_blocks());
  hash_table_ = std::make_shared<
      phmap::flat_hash_map<int64_t, std::shared_ptr<std::vector<RecordID>>>>();
}

void SelectBuildHash::execute(Task *ctx) {
  chunk_row_offsets_.resize(table_->get_num_rows());
  chunk_row_offsets_[0] = 0;
  for (std::size_t i = 1; i < table_->get_num_blocks(); i++) {
    chunk_row_offsets_[i] =
        chunk_row_offsets_[i - 1] + table_->get_block(i - 1)->get_num_rows();
  }

  hash_table_->reserve(table_->get_num_rows());

  ctx->spawnTask(CreateTaskChain(
      // Task 1: perform selection on all blocks
      CreateLambdaTask([this](Task *internal) {
        table_->ForEachBatch([&](auto batch_index, auto batch_size) {
          internal->spawnLambdaTask([this, batch_index, batch_size]() {
            size_t base_index = batch_index * batch_size;
            size_t batch_limit =
                std::min((base_index + batch_size), table_->get_num_blocks());
            for (size_t block_index = base_index; block_index < batch_limit;
                 block_index++) {
              this->ExecuteBlock(block_index);
            }
          });
        });
      }),
      // Task 2: create the output result
      CreateLambdaTask([this](Task *internal) {
        std::shared_ptr<arrow::ChunkedArray> chunked_filter;
        if (tree_ != nullptr) {
          chunked_filter = std::make_shared<arrow::ChunkedArray>(filters_);
        }
        auto lazy_table =
            tree_ != nullptr
                ? std::make_shared<LazyTable>(table_, chunked_filter,
                                              arrow::Datum(), arrow::Datum())
                : std::make_shared<LazyTable>(table_, arrow::Datum(),
                                              arrow::Datum(), arrow::Datum());

        lazy_table->set_hash_table(hash_table_);
        output_result_->append(lazy_table);
      })));
}

void SelectBuildHash::ExecuteBlock(int block_index) {
  auto block = table_->get_block(block_index);
  auto block_data = block->get_column_by_name(join_column_.col_name)
                        ->data()
                        ->GetValues<uint64_t>(1, 0);

  std::unordered_map<int64_t, std::vector<RecordID>> local_table;

  auto update_table = [&](uint32_t row_id) {
    auto key_value_pair = local_table.find(block_data[row_id]);
    if (key_value_pair != local_table.end()) {
      local_table[block_data[row_id]].push_back(
          {(uint32_t)chunk_row_offsets_[block_index] + row_id,
           (uint16_t)block_index});
    } else {
      local_table[block_data[row_id]] = {
          {(uint32_t)chunk_row_offsets_[block_index] + row_id,
           (uint16_t)block_index}};
    }
  };
  if (tree_ != nullptr) {
    auto block_filter = Select::Filter(block, tree_->root_);
    filters_[block_index] = block_filter.make_array();

    auto filter_data = filters_[block_index]->data()->GetValues<uint8_t>(1, 0);

    for (uint32_t row = 0; row < block->get_num_rows(); row++) {
      if (arrow::BitUtil::GetBit(filter_data, row)) {
        update_table(row);
      }
    }
  } else {
    for (std::uint32_t row = 0; row < block->get_num_rows(); row++) {
      update_table(row);
    }
  }

  {
    // critical section for updating the global map
    // TODO (@suryadev) : Try to move to better thread-safe map structures,
    // since we use a list as value
    std::lock_guard<std::mutex> guard(table_lock);
    for (auto const &[key, val] : local_table) {
      auto key_value_pair = hash_table_->find(key);
      if (key_value_pair != hash_table_->end()) {
        (*hash_table_)[key]->insert((*hash_table_)[key]->end(), val.begin(),
                                    val.end());
      } else {
        (*hash_table_)[key] = std::make_shared<std::vector<RecordID>>(val);
      }
    }
  }
}

}  // namespace operators
}  // namespace hustle
