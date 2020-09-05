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

#include "operators/join.h"

#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <iostream>
#include <utility>

#include "storage/util.h"
#include "utils/bloom_filter.h"

namespace hustle::operators {

Join::Join(const std::size_t query_id,
           std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
           std::shared_ptr<OperatorResult> output_result, JoinGraph graph)
    : Operator(query_id),
      prev_result_vec_(prev_result_vec),
      output_result_(output_result),
      graph_(graph) {
  prev_result_ = std::make_shared<OperatorResult>();
  joined_indices_.resize(2);
  joined_index_chunks_.resize(2);
}

void Join::execute(Task *ctx) {
  for (auto &result : prev_result_vec_) {
    prev_result_->append(result);
  }
  // To handle a variable number of joins, we must store the tasks beforehand.
  // The variadic CreateTaskChain cannot help us here!
  std::vector<Task *> tasks;

  // TODO(nicholas): For now, we assume joins have simple predicates
  //   without connective operators.
  auto predicates = graph_.get_predicates(0);

  // Loop over the join predicates and store the left/right LazyTables and the
  // left/right join column names
  for (auto &jpred : predicates) {
    auto left_ref = jpred.left_col_ref_;
    auto right_ref = jpred.right_col_ref_;

    left_col_names_.push_back(left_ref.col_name);
    right_col_names_.push_back(right_ref.col_name);

    // The previous result prev may contain many LazyTables. Find the
    // LazyTables that we want to join.
    for (std::size_t i = 0; i < prev_result_->lazy_tables_.size(); i++) {
      auto lazy_table = prev_result_->get_table(i);
      finished_[lazy_table.table] = false;

      if (left_ref.table == lazy_table.table) {
        lefts_.push_back(lazy_table);
      } else if (right_ref.table == lazy_table.table) {
        rights_.push_back(lazy_table);
      }
    }
  }

  // Each task is one join
  for (std::size_t join_id = 0; join_id < lefts_.size(); join_id++) {
    tasks.push_back(CreateLambdaTask(
        [this, join_id](Task *internal) { HashJoin(join_id, internal); }));
  }

  tasks.push_back(CreateLambdaTask([this]() { Finish(); }));
  // The (j+1)st task in tasks is not executed until the jth task finishes.
  ctx->spawnTask(CreateTaskChain(tasks));
}

void Join::HashJoin(int join_id, Task *ctx) {
  // Join lefts[join_id] with rights[join_id].
  // Why pass in an index i instead of the actual left and right tables?
  // If we pass the tables to the lambda expression by value, then updates
  // made to the index arrays will not be seen by downstream joins. If we
  // pass the tables by reference, then we get a nullptr exception, since
  // the left and right tables would go out of scope before the Task can be
  // executed. To get around this issue, we store the left and right tables
  // in vectors and access them by index. Because the vectors are class
  // variables (and because we pass `this` by reference), updates to the index
  // arrays will be seen by downstream joins, and we don't have to worry about
  // anything going out of scope.
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this, join_id](Task *internal) {
        left_ = prev_result_->get_table(lefts_[join_id].table);
        right_ = prev_result_->get_table(rights_[join_id].table);
        left_.get_column_by_name(internal, left_col_names_[join_id],
                                 left_join_col_);
        right_.get_column_by_name(internal, right_col_names_[join_id],
                                  right_join_col_);
      }),
      CreateLambdaTask([this, join_id](Task *internal) {
        // Build phase
        if (right_.filter.kind() == arrow::Datum::CHUNKED_ARRAY) {
          BuildHashTable(right_join_col_.chunked_array(),
                         right_.filter.chunked_array(), internal);
        } else {
          BuildHashTable(right_join_col_.chunked_array(), nullptr, internal);
        }
      }),
      CreateLambdaTask([this, join_id](Task *internal) {
        // Probe phase
        ProbeHashTable(left_join_col_.chunked_array(), left_.filter,
                       left_.indices, internal);
      }),
      CreateLambdaTask([this, join_id](Task *internal) {
        FinishProbe(internal);
        finished_[lefts_[join_id].table] = true;
        finished_[rights_[join_id].table] = true;
      }),
      CreateLambdaTask([this, join_id](Task *internal) {
        auto left = prev_result_->get_table(lefts_[join_id].table);
        auto right = prev_result_->get_table(rights_[join_id].table);
        // Update indices of other LazyTables in the previous OperatorResult
        prev_result_ = BackPropogateResult(left, right, joined_indices_);
      })));
}  // namespace hustle::operators

void Join::BuildHashTable(const std::shared_ptr<arrow::ChunkedArray> &col,
                          const std::shared_ptr<arrow::ChunkedArray> &filter,
                          Task *ctx) {
  // NOTE: Do not forget to clear the hash table
  hash_table_.clear();

  // Precompute the row offsets of each chunk. A multithreaded build phase
  // requires that we know all offsets beforehand.
  std::vector<uint64_t> chunk_row_offsets(col->num_chunks());
  chunk_row_offsets[0] = 0;
  for (std::size_t i = 1; i < col->num_chunks(); i++) {
    chunk_row_offsets[i] =
        chunk_row_offsets[i - 1] + col->chunk(i - 1)->length();
  }

  size_t hash_table_size = col->length();
  if (filter != nullptr) {
    uint32_t length_after_filtering = 0;

    for (int j = 0; j < col->num_chunks(); ++j) {
      length_after_filtering += arrow::compute::internal::GetFilterOutputSize(
          *filter->chunk(j)->data(),
          arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
    }
    hash_table_size = length_after_filtering;
  }
  hash_table_.reserve(hash_table_size);

  for (std::size_t i = 0; i < col->num_chunks(); i++) {
    // Each task inserts one chunk into the hash table
    // TODO(nicholas): for now, we assume the join column is INT64 type.
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));
    const uint8_t *filter_data = nullptr;
    if (filter != nullptr) {
      filter_data = filter->chunk(i)->data()->GetValues<uint8_t>(1, 0);
    }
    for (std::uint32_t row = 0; row < chunk->length(); row++) {
      if (filter_data == nullptr || arrow::BitUtil::GetBit(filter_data, row)) {
        hash_table_[chunk->Value(row)] = {(uint32_t)chunk_row_offsets[i] + row,
                                          (uint16_t)i};
      }
    }
  }
}

void Join::ProbeHashTableBlock(
    const std::shared_ptr<arrow::ChunkedArray> &probe_col,
    const std::shared_ptr<arrow::ChunkedArray> &probe_filter, int batch_i,
    int batch_size, std::vector<uint64_t> chunk_row_offsets) {
  int base_i = batch_i * batch_size;
  auto hash_table_end = hash_table_.end();

  // TODO(nicholas): for now, we assume the join column is fixed width type,
  // i.e. values are stored in the buffer at index 1.

  for (size_t i = base_i;
       i < base_i + batch_size && i < probe_col->num_chunks(); ++i) {
    arrow::Status status;

    int num_joined_indices = 0;
    auto offset = chunk_row_offsets[i];
    auto chunk = probe_col->chunk(i);
    auto chunk_length = chunk->length();
    auto left_join_chunk_data = chunk->data()->GetValues<uint64_t>(1, 0);
    const uint8_t *filter_data = nullptr;

    if (probe_filter != nullptr) {
      filter_data =
          (const uint8_t *)probe_filter->chunk(i)->data()->GetValues<uint8_t>(
              1, 0);
    }

    // The indices of the rows joined in chunk i
    auto *joined_left_indices =
        (uint32_t *)malloc(sizeof(uint32_t) * chunk_length);
    auto *joined_right_indices =
        (uint32_t *)malloc(sizeof(uint32_t) * chunk_length);
    auto *joined_left_index_chunks =
        (uint16_t *)malloc(sizeof(uint16_t) * chunk_length);
    auto *joined_right_index_chunks =
        (uint16_t *)malloc(sizeof(uint16_t) * chunk_length);

    for (std::size_t row = 0; row < chunk_length; row++) {
      if (filter_data == nullptr || arrow::BitUtil::GetBit(filter_data, row)) {
        auto key_value_pair = hash_table_.find(left_join_chunk_data[row]);
        if (key_value_pair != hash_table_end) {
          joined_left_indices[num_joined_indices] = row + offset;
          joined_left_index_chunks[num_joined_indices] = i;

          joined_right_indices[num_joined_indices] =
              key_value_pair->second.index;
          joined_right_index_chunks[num_joined_indices] =
              key_value_pair->second.chunk;
          ++num_joined_indices;
        }
      }
    }

    new_left_indices_vector_[i] = std::vector<uint32_t>(
        joined_left_indices, joined_left_indices + num_joined_indices);
    new_right_indices_vector_[i] = std::vector<uint32_t>(
        joined_right_indices, joined_right_indices + num_joined_indices);

    left_index_chunks_vector_[i] =
        std::vector<uint16_t>(joined_left_index_chunks,
                              joined_left_index_chunks + num_joined_indices);
    right_index_chunks_vector_[i] =
        std::vector<uint16_t>(joined_right_index_chunks,
                              joined_right_index_chunks + num_joined_indices);

    free(joined_left_indices);
    free(joined_right_indices);
    free(joined_left_index_chunks);
    free(joined_right_index_chunks);
  }
}

void Join::ProbeHashTable(const std::shared_ptr<arrow::ChunkedArray> &probe_col,
                          const arrow::Datum &probe_filter,
                          const arrow::Datum &probe_indices, Task *ctx) {
  new_left_indices_vector_.resize(probe_col->num_chunks());
  new_right_indices_vector_.resize(probe_col->num_chunks());
  right_index_chunks_vector_.resize(probe_col->num_chunks());
  left_index_chunks_vector_.resize(probe_col->num_chunks());

  // Precompute row offsets. A multithreaded probe phase requires that we know
  // all offsets beforehand.
  std::vector<uint64_t> chunk_row_offsets(probe_col->num_chunks());
  chunk_row_offsets[0] = 0;
  for (std::size_t i = 1; i < probe_col->num_chunks(); i++) {
    chunk_row_offsets[i] =
        chunk_row_offsets[i - 1] + probe_col->chunk(i - 1)->length();
  }

  int batch_size =
      probe_col->num_chunks() / std::thread::hardware_concurrency() / 2;
  if (batch_size == 0) batch_size = probe_col->num_chunks();
  int num_batches = probe_col->num_chunks() / batch_size +
                    1;  // if num_chunks is a multiple of batch_size, we don't
                        // actually want the +1
  if (num_batches == 0) num_batches = 1;

  // Probe phase
  for (std::size_t batch_i = 0; batch_i < num_batches; batch_i++) {
    // Each task probes one chunk
    ctx->spawnLambdaTask([this, batch_i, batch_size, probe_col, probe_filter,
                          probe_indices, chunk_row_offsets] {
      if (probe_filter.kind() == arrow::Datum::CHUNKED_ARRAY) {
        ProbeHashTableBlock(probe_col, probe_filter.chunked_array(), batch_i,
                            batch_size, chunk_row_offsets);
      } else {
        ProbeHashTableBlock(probe_col, nullptr, batch_i, batch_size,
                            chunk_row_offsets);
      }
    });
  }
}  // namespace hustle::operators

void Join::FinishProbe(Task *ctx) {
  int num_indices = 0;
  for (auto &vec : new_left_indices_vector_) {
    num_indices += vec.size();
  }

  ctx->spawnLambdaTask([this, num_indices] {
    arrow::Status status;
    arrow::UInt32Builder new_left_indices_builder;
    std::shared_ptr<arrow::UInt32Array> new_left_indices;

    status = new_left_indices_builder.Reserve(num_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    // TODO(nicholas): Use UnsafeAppend or index access
    // Append all of the indices to an ArrayBuilder.
    for (std::size_t i = 0; i < new_left_indices_vector_.size(); i++) {
      status =
          new_left_indices_builder.AppendValues(new_left_indices_vector_[i]);
      evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }

    status = new_left_indices_builder.Finish(&new_left_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    joined_indices_[0] = (arrow::Datum(new_left_indices));
  });
  ctx->spawnLambdaTask([this, num_indices] {
    arrow::Status status;

    arrow::UInt32Builder new_right_indices_builder;
    std::shared_ptr<arrow::UInt32Array> new_right_indices;

    status = new_right_indices_builder.Reserve(num_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    // TODO(nicholas): Use UnsafeAppend or index access
    // Append all of the indices to an ArrayBuilder.
    for (int i = 0; i < new_right_indices_vector_.size(); i++) {
      status =
          new_right_indices_builder.AppendValues(new_right_indices_vector_[i]);
      evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }

    status = new_right_indices_builder.Finish(&new_right_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    joined_indices_[1] = (arrow::Datum(new_right_indices));
  });
}

std::shared_ptr<OperatorResult> Join::BackPropogateResult(
    LazyTable &left, LazyTable right,
    const std::vector<arrow::Datum> &joined_indices) {
  arrow::Status status;
  arrow::Datum new_indices;
  arrow::Datum new_index_chunks;

  std::vector<LazyTable> output_lazy_tables;

  // The indices of the indices that were joined
  auto left_indices_of_indices = joined_indices_[0];
  auto right_indices_of_indices = joined_indices_[1];

  auto left_index_chunks = joined_index_chunks_[0];
  auto right_index_chunks = joined_index_chunks_[1];

  // Assume that indices are correct and that boundschecking is unecessary.
  // CHANGE TO TRUE IF YOU ARE DEBUGGING
  arrow::compute::TakeOptions take_options(true);

  // Update the indices of the left LazyTable. If there was no previous
  // join on the left table, then left_indices_of_indices directly
  // corresponds to indices in the left table, and we do not need to
  // call Take.

  if (left.indices.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Take(left.indices, left_indices_of_indices,
                                  take_options)
                 .Value(&new_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  } else {
    new_indices = left_indices_of_indices;
  }
  if (left.index_chunks.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Take(left.index_chunks, left_indices_of_indices,
                                  take_options)
                 .Value(&new_index_chunks);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  } else {
    new_index_chunks = left_index_chunks;
  }
  output_lazy_tables.emplace_back(left.table, left.filter, new_indices,
                                  new_index_chunks);

  // Update the indices of the right LazyTable. If there was no previous
  // join on the right table, then right_indices_of_indices directly
  // corresponds to indices in the right table, and we do not need to
  // call Take.
  if (right.indices.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Take(right.indices, right_indices_of_indices,
                                  take_options)
                 .Value(&new_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  } else {
    new_indices = right_indices_of_indices;
  }
  new_index_chunks = arrow::Datum();
  output_lazy_tables.emplace_back(right.table, right.filter, new_indices,
                                  new_index_chunks);

  // Propogate the join to the other tables in the previous OperatorResult.
  // This elimates tuples from other tables in the previous result that were
  // eliminated in the most recent join.
  for (auto &lazy_table : prev_result_->lazy_tables_) {
    if (lazy_table.table != left.table && lazy_table.table != right.table) {
      if (finished_[lazy_table.table]) {
        status = arrow::compute::Take(lazy_table.indices,
                                      left_indices_of_indices, take_options)
                     .Value(&new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        output_lazy_tables.emplace_back(lazy_table.table, lazy_table.filter,
                                        new_indices, arrow::Datum());
      } else {
        output_lazy_tables.emplace_back(lazy_table.table, lazy_table.filter,
                                        lazy_table.indices,
                                        lazy_table.index_chunks);
      }
    }
  }

  return std::make_shared<OperatorResult>(output_lazy_tables);
}

void Join::Finish() {
  // Must append to output_result_ first
  output_result_->append(prev_result_);
}

}  // namespace hustle::operators