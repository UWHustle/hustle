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

#include "operators/fused/fused_join.h"

#include <arrow/compute/api.h>
#include <arrow/scalar.h>

#include <iostream>
#include <utility>

#include "storage/util.h"
#include "utils/bloom_filter.h"

namespace hustle::operators {

static const uint32_t kLeftJoinIndex = 0;
static const uint32_t kRightJoinIndex = 1;

FusedJoin::FusedJoin(
    const std::size_t query_id,
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

void FusedJoin::BuildFilters(Task *ctx) {
  right_filters_.resize(rights_.size());
  dim_pk_cols_.reserve(rights_.size());
  for (int i = 0; i < rights_.size(); i++) {
    auto dim_join_col_name = right_col_names_[i];
    auto col = rights_[i].table->get_column_by_name(dim_join_col_name);
    // Task = build the Bloom filter for one dimension table.
    ctx->spawnTask(CreateTaskChain(CreateLambdaTask([this, dim_join_col_name,
                                                     col, i] {
      auto pk_col = col;
      auto filter_d = rights_[i].filter;
      std::shared_ptr<BloomFilter> bloom_filter;

      // TODO(nicholas): consider indices as well. We don't have to worry
      // about this for SSB, though.
      auto indices_d = rights_[i].indices;

      if (filter_d.kind() == arrow::Datum::NONE) {
        bloom_filter = std::make_shared<BloomFilter>(pk_col->length());
        for (int j = 0; j < pk_col->num_chunks(); ++j) {
          // TODO(nicholas): For now, we assume the column is of INT64 type.
          auto chunk =
              std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
          for (int k = 0; k < chunk->length(); ++k) {
            bloom_filter->insert(chunk->Value(k));
          }
        }
      } else {
        auto filter = filter_d.chunked_array();
        uint32_t length_after_filtering = 0;

        for (int j = 0; j < pk_col->num_chunks(); ++j) {
          length_after_filtering +=
              arrow::compute::internal::GetFilterOutputSize(
                  *filter->chunk(j)->data(),
                  arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
        }
        bloom_filter = std::make_shared<BloomFilter>(length_after_filtering);

        for (int j = 0; j < pk_col->num_chunks(); ++j) {
          // TODO(nicholas): For now, we assume the column is of INT64 type.
          auto chunk =
              std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
          auto chunkf =
              std::static_pointer_cast<arrow::BooleanArray>(filter->chunk(j));

          for (int k = 0; k < chunk->length(); ++k) {
            if (chunkf->Value(k)) {
              bloom_filter->insert(chunk->Value(k));
            }
          }
        }
      }
      bloom_filter->set_memory(1);
      right_filters_[i] = bloom_filter;
    })));
  }
}

void FusedJoin::execute(Task *ctx) {
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

  hash_tables_.resize(rights_.size());
  // Each task is one join
  tasks.push_back(
      CreateLambdaTask([this](Task *internal) { BuildFilters(internal); }));
  for (std::size_t join_id = 0; join_id < lefts_.size(); join_id++) {
    tasks.push_back(CreateLambdaTask(
        [this, join_id](Task *internal) { HashJoin(join_id, internal); }));
  }

  tasks.push_back(CreateLambdaTask([this]() { Finish(); }));
  // The (j+1)st task in tasks is not executed until the jth task finishes.
  ctx->spawnTask(CreateTaskChain(tasks));
}

void FusedJoin::HashJoin(int join_id, Task *ctx) {
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
        if (right_.hash_table_ != nullptr) {
          hash_tables_[join_id] = right_.hash_table_;
        }
      }),
      CreateLambdaTask([this, join_id](Task *internal) {
        // Probe phase
        ProbeHashTable(join_id, left_join_col_.chunked_array(), left_.filter,
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

void FusedJoin::ProbeHashTableBlock(
    int join_id, const std::shared_ptr<arrow::ChunkedArray> &probe_col,
    const std::shared_ptr<arrow::ChunkedArray> &probe_filter, int batch_i,
    int batch_size, std::vector<uint64_t> chunk_row_offsets) {
  int base_i = batch_i * batch_size;
  auto hash_table_end = hash_tables_[join_id]->end();
  auto bloom_filter = right_filters_[join_id];

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

    if (filter_data != nullptr) {
      for (std::size_t row = 0; row < chunk_length; row++) {
        if (arrow::BitUtil::GetBit(filter_data, row)) {
          auto left_value = left_join_chunk_data[row];
          if (bloom_filter->probe(left_value)) {
            auto key_value_pair = hash_tables_[join_id]->find(left_value);
            if (key_value_pair != hash_table_end) {
              joined_left_indices[num_joined_indices] = row + offset;
              joined_right_indices[num_joined_indices] =
                  key_value_pair->second.index;
              ++num_joined_indices;
            }
          }
        }
      }
    } else {
      for (std::size_t row = 0; row < chunk_length; row++) {
        auto left_value = left_join_chunk_data[row];
        if (bloom_filter->probe(left_value)) {
          auto key_value_pair = hash_tables_[join_id]->find(left_value);
          if (key_value_pair != hash_table_end) {
            joined_left_indices[num_joined_indices] = row + offset;
            joined_right_indices[num_joined_indices] =
                key_value_pair->second.index;
            ++num_joined_indices;
          }
        }
      }
    }

    new_left_indices_vector_[i] = std::vector<uint32_t>(
        joined_left_indices, joined_left_indices + num_joined_indices);
    new_right_indices_vector_[i] = std::vector<uint32_t>(
        joined_right_indices, joined_right_indices + num_joined_indices);

    free(joined_left_indices);
    free(joined_right_indices);
  }
}

void FusedJoin::ProbeHashTable(
    int join_id, const std::shared_ptr<arrow::ChunkedArray> &probe_col,
    const arrow::Datum &probe_filter, const arrow::Datum &probe_indices,
    Task *ctx) {
  new_left_indices_vector_.resize(probe_col->num_chunks());
  new_right_indices_vector_.resize(probe_col->num_chunks());

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
    ctx->spawnLambdaTask([this, join_id, batch_i, batch_size, probe_col,
                          probe_filter, probe_indices, chunk_row_offsets] {
      if (probe_filter.kind() == arrow::Datum::CHUNKED_ARRAY) {
        ProbeHashTableBlock(join_id, probe_col, probe_filter.chunked_array(),
                            batch_i, batch_size, chunk_row_offsets);
      } else {
        ProbeHashTableBlock(join_id, probe_col, nullptr, batch_i, batch_size,
                            chunk_row_offsets);
      }
    });
  }
}  // namespace hustle::operators

void FusedJoin::FinishProbe(Task *ctx) {
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
    joined_indices_[kLeftJoinIndex] = (arrow::Datum(new_left_indices));
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

    joined_indices_[kRightJoinIndex] = (arrow::Datum(new_right_indices));
  });
}

std::shared_ptr<OperatorResult> FusedJoin::BackPropogateResult(
    LazyTable &left, LazyTable right,
    const std::vector<arrow::Datum> &joined_indices) {
  arrow::Status status;
  arrow::Datum new_indices;
  arrow::Datum new_index_chunks;

  std::vector<LazyTable> output_lazy_tables;

  // The indices of the indices that were joined
  auto left_indices_of_indices = joined_indices_[kLeftJoinIndex];
  auto right_indices_of_indices = joined_indices_[kRightJoinIndex];

  auto left_index_chunks = joined_index_chunks_[kLeftJoinIndex];
  auto right_index_chunks = joined_index_chunks_[kRightJoinIndex];

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
  /*if (left.index_chunks.kind() != arrow::Datum::NONE) {
    status = arrow::compute::Take(left.index_chunks, left_indices_of_indices,
                                  take_options)
                 .Value(&new_index_chunks);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  } else {
    new_index_chunks = left_index_chunks;
  }*/
  new_index_chunks = left_index_chunks;
  output_lazy_tables.emplace_back(left.table, left.filter, new_indices,
                                  new_index_chunks, left.hash_table_);

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
                                  new_index_chunks, right.hash_table_);

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
                                        new_indices, arrow::Datum(),
                                        lazy_table.hash_table_);
      } else {
        output_lazy_tables.emplace_back(
            lazy_table.table, lazy_table.filter, lazy_table.indices,
            lazy_table.index_chunks, lazy_table.hash_table_);
      }
    }
  }

  return std::make_shared<OperatorResult>(output_lazy_tables);
}

void FusedJoin::Finish() {
  // Must append to output_result_ first
  output_result_->append(prev_result_);
}

}  // namespace hustle::operators