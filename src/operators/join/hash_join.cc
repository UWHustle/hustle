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

#include "operators/join/hash_join.h"

#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <utility>

#include "storage/utils/util.h"
#include "utils/bloom_filter.h"
#include "utils/parallel_utils.h"

#define LEFT_JOIN_REF_IDX 0
#define RIGHT_JOIN_REF_IDX 1

namespace hustle::operators {

HashJoin::HashJoin(
    const std::size_t query_id,
    std::shared_ptr<std::vector<OperatorResult::OpResultPtr>> prev_result,
    OperatorResult::OpResultPtr output_result,
    std::shared_ptr<JoinPredicate> predicate)
    : HashJoin(query_id, prev_result, output_result, predicate,
               std::make_shared<OperatorOptions>()) {}

HashJoin::HashJoin(
    const std::size_t query_id,
    std::shared_ptr<std::vector<OperatorResult::OpResultPtr>> prev_result,
    OperatorResult::OpResultPtr output_result,
    std::shared_ptr<JoinPredicate> predicate,
    std::shared_ptr<OperatorOptions> options)
    : Operator(query_id, options),
      prev_result_(prev_result),
      output_result_(output_result),
      predicate_(predicate) {
  // prev result has two operator result - 0 - left, 1 - right
  assert(prev_result->size() == 2);
  joined_indices_.resize(2);
}

void HashJoin::Execute(Task *ctx, int32_t flags) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task *internal) { Initialize(internal); }),
      CreateLambdaTask([&, this](Task *internal) { Join(internal); }),
      CreateLambdaTask([&, this](Task *internal) { Finish(); })));
}

void HashJoin::Initialize(Task *ctx) {
  left_table_ = prev_result_->at(LEFT_JOIN_REF_IDX)
                    ->get_table(predicate_->left_col_.table);
  right_table_ = prev_result_->at(RIGHT_JOIN_REF_IDX)
                     ->get_table(predicate_->right_col_.table);

  left_table_->MaterializeColumn(ctx, predicate_->left_col_.col_name, lcol_);
  right_table_->MaterializeColumn(ctx, predicate_->right_col_.col_name, rcol_);
}

void HashJoin::Join(Task *ctx) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([&, this](Task *internal) {
        // Build phase
        if (right_table_->hash_table() != nullptr) {
          hash_table_ = right_table_->hash_table();
        } else {
          BuildHashTable(
              rcol_.chunked_array(),
              (right_table_->filter.kind() == arrow::Datum::CHUNKED_ARRAY)
                  ? right_table_->filter.chunked_array()
                  : nullptr,
              internal);
        }
      }),
      CreateLambdaTask([&, this](Task *internal) {
        ProbeHashTable(lcol_.chunked_array(), left_table_->filter,
                       left_table_->indices, internal);
      }),
      CreateLambdaTask([this](Task *internal) { FinishProbe(internal); })));
}

void HashJoin::BuildHashTable(
    const std::shared_ptr<arrow::ChunkedArray> &col,
    const std::shared_ptr<arrow::ChunkedArray> &filter, Task *ctx) {
  hash_table_ = std::make_shared<
      phmap::flat_hash_map<int64_t, std::shared_ptr<std::vector<RecordID>>>>();

  // Precompute the row offsets of each chunk. A multithreaded build phase
  // requires that we know all offsets beforehand.
  std::vector<uint64_t> chunk_offsets(col->num_chunks());
  ComputeChunkOffsets(chunk_offsets, col);

  size_t hash_table_size = col->length();
  if (filter != nullptr) {
    uint32_t updated_length = 0;

    for (int j = 0; j < col->num_chunks(); ++j) {
      updated_length += arrow::compute::internal::GetFilterOutputSize(
          *filter->chunk(j)->data(),
          arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
    }
    hash_table_size = updated_length;
  }
  hash_table_->reserve(hash_table_size);

  for (std::size_t i = 0; i < col->num_chunks(); i++) {
    // Each task inserts one chunk into the hash table
    // TODO: for now, we assume the join column is INT64 type.
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));
    const uint8_t *filter_data = nullptr;
    if (filter != nullptr) {
      filter_data = filter->chunk(i)->data()->GetValues<uint8_t>(1, 0);
    }
    for (std::uint32_t row = 0; row < chunk->length(); row++) {
      if (filter_data == nullptr || arrow::BitUtil::GetBit(filter_data, row)) {
        auto record_id_itr = hash_table_->find(chunk->Value(row));
        if (record_id_itr != hash_table_->end()) {
          auto record_ids = record_id_itr->second;
          record_ids->push_back(
              {(uint32_t)chunk_offsets[i] + row, (uint16_t)i});
          (*hash_table_)[chunk->Value(row)] = record_ids;
        } else {
          (*hash_table_)[chunk->Value(row)] =
              std::make_shared<std::vector<RecordID>>(std::vector<RecordID>(
                  {{(uint32_t)chunk_offsets[i] + row, (uint16_t)i}}));
        }
      }
    }
  }
}

void HashJoin::ProbeHashTableBlock(
    const std::shared_ptr<arrow::ChunkedArray> &probe_col,
    const std::shared_ptr<arrow::ChunkedArray> &probe_filter, int batch_idx,
    int batch_size, std::vector<uint64_t> chunk_row_offsets) {
  int base_idx = batch_idx * batch_size;
  auto hash_table_end = hash_table_->end();
  for (size_t i = base_idx;
       (i < base_idx + batch_size && i < probe_col->num_chunks()); ++i) {
    int num_indices = 0;

    auto chunk = probe_col->chunk(i);
    auto left_data = chunk->data()->GetValues<uint64_t>(1, 0);
    const uint8_t *filter = nullptr;

    if (probe_filter != nullptr) {
      auto filter_chunk = probe_filter->chunk(i);
      filter = (const uint8_t *)filter_chunk->data()->GetValues<uint8_t>(1, 0);
    }

    // The indices of the rows joined in chunk i
    int64_t chunk_len = chunk->length();
    uint32_t *l_idxs = (uint32_t *)malloc(sizeof(uint32_t) * chunk_len);
    uint32_t *r_idxs = (uint32_t *)malloc(sizeof(uint32_t) * chunk_len);
    auto idx_len = chunk_len;
    auto offset = chunk_row_offsets[i];
    for (auto row = 0; row < chunk_len; row++) {
      if (filter == nullptr || arrow::BitUtil::GetBit(filter, row)) {
        auto record_itr = hash_table_->find(left_data[row]);

        if (record_itr != hash_table_end) {
          for (auto record_id : *(record_itr->second)) {
            if (idx_len <= num_indices) {
              idx_len <<= 1;
              l_idxs = (uint32_t *)realloc(l_idxs, sizeof(uint32_t) * idx_len);
              r_idxs = (uint32_t *)realloc(r_idxs, sizeof(uint32_t) * idx_len);
            }
            l_idxs[num_indices] = row + offset;
            r_idxs[num_indices] = record_id.index;
            ++num_indices;
          }
        }
      }
    }

    left_indices_[i] = std::vector<uint32_t>(l_idxs, l_idxs + num_indices);
    right_indices_[i] = std::vector<uint32_t>(r_idxs, r_idxs + num_indices);

    free(l_idxs);
    free(r_idxs);
  }
}

void HashJoin::ProbeHashTable(
    const std::shared_ptr<arrow::ChunkedArray> &probe_col,
    const arrow::Datum &probe_filter, const arrow::Datum &probe_indices,
    Task *ctx) {
  left_indices_.resize(probe_col->num_chunks());
  right_indices_.resize(probe_col->num_chunks());

  // Precompute row offsets. A multithreaded probe phase requires that we know
  // all offsets beforehand.
  std::vector<uint64_t> chunk_offsets(probe_col->num_chunks());
  ComputeChunkOffsets(chunk_offsets, probe_col);

  int num_batches, batch_size;
  utils::ComputeBatchConfig(
      batch_size, num_batches, probe_col->num_chunks(),
      (std::thread::hardware_concurrency() * options_->get_parallel_factor()));

  for (std::size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
    // Each task probes one chunk
    ctx->spawnLambdaTask([this, batch_idx, batch_size, &probe_col,
                          &probe_filter, chunk_offsets] {
      if (probe_filter.kind() == arrow::Datum::CHUNKED_ARRAY) {
        ProbeHashTableBlock(probe_col, probe_filter.chunked_array(), batch_idx,
                            batch_size, chunk_offsets);
      } else {
        ProbeHashTableBlock(probe_col, nullptr, batch_idx, batch_size,
                            chunk_offsets);
      }
    });
  }
}

void HashJoin::FinishProbe(Task *ctx) {
  int num_indices = 0;
  for (auto &vec : left_indices_) {
    num_indices += vec.size();
  }

  auto create_join_indices =
      [this, num_indices](int32_t tbl_index,
                          std::vector<std::vector<uint32_t>> &indices_vector) {
        arrow::Status status;
        arrow::UInt32Builder indices_builder;
        std::shared_ptr<arrow::UInt32Array> indices;

        status = indices_builder.Reserve(num_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        for (auto indices_elem : indices_vector) {
          status = indices_builder.AppendValues(indices_elem);
          evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }
        status = indices_builder.Finish(&indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        joined_indices_[tbl_index] = (arrow::Datum(indices));
      };
  create_join_indices(LEFT_JOIN_REF_IDX, left_indices_);
  create_join_indices(RIGHT_JOIN_REF_IDX, right_indices_);
}

OperatorResult::OpResultPtr HashJoin::BackPropogateResult(
    const std::vector<arrow::Datum> &joined_indices) {
  // The indices of the indices that were joined
  auto left_indices = joined_indices_[LEFT_JOIN_REF_IDX];
  auto right_indices = joined_indices_[RIGHT_JOIN_REF_IDX];

  arrow::compute::TakeOptions take_options(true);
  arrow::Status status;

  arrow::Datum indices, index_chunks;
  std::vector<LazyTable::LazyTablePtr> output_lazy_tables;

  /**
   * Update the indices of the LazyTable. If there was no previous
   * join on the table, then update indices directly
   * corresponds to indices in the left table, and we do not need to
   * call Take.
   */
  auto update_indices = [&, this](LazyTable::LazyTablePtr &table,
                                  arrow::Datum &join_indices) {
    if (table->indices.kind() != arrow::Datum::NONE) {
      status = arrow::compute::Take(table->indices, join_indices, take_options)
                   .Value(&indices);
      evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    } else {
      indices = join_indices;
    }
    output_lazy_tables.emplace_back(
        std::make_shared<LazyTable>(table->table, arrow::Datum(), indices,
                                    index_chunks, table->hash_table()));
  };

  /**
   *  Propogate the join to the other tables in the previous OperatorResult.
   *  This elimates tuples from other tables in the previous result that were
   *  eliminated in the most recent join.
   */
  auto propogate_indices = [&, this](int index, DBTable::TablePtr table,
                                     arrow::Datum &join_indices) {
    for (auto &lazy_table : prev_result_->at(index)->lazy_tables_) {
      if (lazy_table->table != table &&
          lazy_table->indices.kind() != arrow::Datum::NONE) {
        status = arrow::compute::Take(lazy_table->indices, join_indices,
                                      take_options)
                     .Value(&indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        output_lazy_tables.emplace_back(std::make_shared<LazyTable>(
            lazy_table->table, arrow::Datum(), indices, index_chunks,
            lazy_table->hash_table()));
      } else {
        output_lazy_tables.emplace_back(std::make_shared<LazyTable>(
            lazy_table->table, arrow::Datum(), join_indices, index_chunks,
            lazy_table->hash_table()));
      }
    }
  };

  update_indices(left_table_, left_indices);
  propogate_indices(LEFT_JOIN_REF_IDX, left_table_->table, left_indices);

  update_indices(right_table_, right_indices);
  propogate_indices(RIGHT_JOIN_REF_IDX, right_table_->table, right_indices);
  return std::make_shared<OperatorResult>(output_lazy_tables);
}

void HashJoin::Finish() {
  // Update indices of other LazyTables in the previous OperatorResult
  output_result_->append(BackPropogateResult(joined_indices_));
}

}  // namespace hustle::operators
