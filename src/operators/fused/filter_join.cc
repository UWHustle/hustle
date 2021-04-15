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

#include "operators/fused/filter_join.h"

#include "arrow_utils.h"

namespace hustle::operators {

FilterJoin::FilterJoin(
    const std::size_t query_id,
    std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
    OperatorResult::OpResultPtr output_result,
    hustle::operators::JoinGraph graph)
    : FilterJoin(query_id, prev_result_vec, output_result, graph,
                 std::make_shared<OperatorOptions>()) {}

FilterJoin::FilterJoin(
    const std::size_t query_id,
    std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
    OperatorResult::OpResultPtr output_result,
    hustle::operators::JoinGraph graph,
    std::shared_ptr<OperatorOptions> options)
    : Operator(query_id, options) {
  prev_result_ = std::make_shared<OperatorResult>();
  prev_result_vec_ = prev_result_vec;
  output_result_ = std::move(output_result);
  graph_ = std::move(graph);
}

void FilterJoin::BuildFilters(Task *ctx) {
  dim_filters_.resize(dim_tables_.size());

  for (size_t table_idx = 0; table_idx < dim_tables_.size(); table_idx++) {
    auto dim_join_col_name = dim_pk_col_names_[table_idx];
    // Task = build the Bloom filter for one dimension table.
    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this, dim_join_col_name, table_idx](Task *internal) {
          dim_pk_cols_[dim_join_col_name] =
              dim_tables_[table_idx].table->get_column_by_name(
                  dim_join_col_name);
        }),
        CreateLambdaTask([this, dim_join_col_name, table_idx] {
          auto pk_col = dim_pk_cols_[dim_join_col_name].chunked_array();
          auto filter_d = dim_tables_[table_idx].filter;
          std::shared_ptr<BloomFilter> bloom_filter;

          // TODO(nicholas): consider indices as well. We don't have to worry
          // about this for SSB, though.
          auto indices_d = dim_tables_[table_idx].indices;

          if (filter_d.kind() == arrow::Datum::NONE) {
            bloom_filter = std::make_shared<BloomFilter>(pk_col->length());
            for (size_t chunk_idx = 0; chunk_idx < pk_col->num_chunks();
                 ++chunk_idx) {
              // TODO(nicholas): For now, we assume the column is of INT64 type.
              auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                  pk_col->chunk(chunk_idx));
              for (int64_t row_idx = 0; row_idx < chunk->length(); ++row_idx) {
                bloom_filter->insert(chunk->Value(row_idx));
              }
            }
          } else {
            auto filter = filter_d.chunked_array();
            uint32_t length_after_filtering = 0;

            for (size_t chunk_idx = 0; chunk_idx < pk_col->num_chunks();
                 ++chunk_idx) {
              length_after_filtering +=
                  arrow::compute::internal::GetFilterOutputSize(
                      *filter->chunk(chunk_idx)->data(),
                      arrow::compute::FilterOptions::NullSelectionBehavior::
                          DROP);
            }
            bloom_filter =
                std::make_shared<BloomFilter>(length_after_filtering);

            for (size_t chunk_idx = 0; chunk_idx < pk_col->num_chunks();
                 ++chunk_idx) {
              // TODO(nicholas): For now, we assume the column is of INT64 type.
              auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                  pk_col->chunk(chunk_idx));
              auto chunkf = std::static_pointer_cast<arrow::BooleanArray>(
                  filter->chunk(chunk_idx));

              for (int64_t row_idx = 0; row_idx < chunk->length(); ++row_idx) {
                if (chunkf->Value(row_idx)) {
                  bloom_filter->insert(chunk->Value(row_idx));
                }
              }
            }
          }

          bloom_filter->set_memory(1);
          bloom_filter->set_fact_fk_name(fact_fk_col_names_[table_idx]);
          if (dim_tables_[table_idx].hash_table() == nullptr) {
            throw "hash table for the dimension relation not constructed";
          }
          dim_filters_[table_idx] = {bloom_filter,
                                     dim_tables_[table_idx].hash_table()};
        })));
  }
}

void FilterJoin::ProbeFilters(int chunk_start, int chunk_end, int filter_j,
                              Task *ctx) {
  // indices[i] stores the indices of fact table rows that passed the
  // ith filter.
  for (auto chunk_i = chunk_start; chunk_i <= chunk_end; ++chunk_i) {
    ctx->spawnLambdaTask([this, chunk_i, filter_j] {
      auto bloom_filter = dim_filters_[filter_j].bloom_filter;
      auto fact_fk_col = fact_fk_cols2_[bloom_filter->get_fact_fk_name()];

      uint32_t *indices = nullptr, *dim_indices = nullptr;
      int32_t indices_length = -1, dim_indices_length = -1;
      uint32_t current_idx_temp, offset = chunk_row_offsets_[chunk_i];

      // TODO(nicholas): For now, we assume the column is of INT64 type
      auto chunk = fact_fk_col->chunk(chunk_i);  // @bug: fact_fk_col is nullptr
      auto chunk_data = chunk->data()->GetValues<int64_t>(1, 0);
      auto chunk_length = chunk->length();

      auto dim_hash_end = dim_filters_[filter_j].hash_table->end();
      // For the first filter, we must probe all rows of the block.
      if (filter_j == 0) {
        // Reserve space for the first index vector
        int join_row_idx = 0;
        indices = (uint32_t *)malloc(sizeof(uint32_t) * chunk_length);
        dim_indices = (uint32_t *)malloc(sizeof(uint32_t) * chunk_length);
        if (dim_filters_[filter_j].hash_table != nullptr) {
          for (int64_t row = 0; row < chunk_length; ++row) {
            if (bloom_filter->probe(chunk_data[row])) {
              auto key_value_pair =
                  dim_filters_[filter_j].hash_table->find(chunk_data[row]);
              // Remember the matched index in the dimension table
              // i.e doing join on the fly while doing look ahead filtering
              if (key_value_pair != dim_hash_end) {
                // FilterJoin supports only join on unique key columns
                assert(key_value_pair->second->size() == 1);
                indices[join_row_idx] = row + offset;
                dim_indices[join_row_idx] = key_value_pair->second->at(0).index;
                join_row_idx++;
              }
            }
          }
        }
        indices_length = join_row_idx - 1;
        lip_indices_[chunk_i] =
            std::vector<uint32_t>(indices, indices + indices_length + 1);
        dim_filters_[filter_j].indices_[chunk_i] = std::vector<uint32_t>(
            dim_indices, dim_indices + indices_length + 1);

        free(indices);
        free(dim_indices);
      }
      // For the remaining filters, we only need to probe rows that passed
      // the previous filters.
      else {
        uint32_t *prev_dim_indices[filter_j];
        for (size_t prev_filter_id = 0; prev_filter_id < filter_j;
             prev_filter_id++) {
          prev_dim_indices[prev_filter_id] =
              dim_filters_[prev_filter_id].indices_[chunk_i].data();
        }
        indices = lip_indices_[chunk_i].data();
        indices_length = lip_indices_[chunk_i].size() - 1;
        dim_indices = (uint32_t *)malloc(sizeof(uint32_t) * chunk_length);

        int join_row_idx = 0;
        if (dim_filters_[filter_j].hash_table != nullptr) {
          while (join_row_idx <= indices_length) {
            auto key = chunk_data[indices[join_row_idx] - offset];
            if (bloom_filter->probe(key)) {
              auto key_value_pair =
                  dim_filters_[filter_j].hash_table->find(key);
              // Remember the matched index in the dimension table
              // i.e doing join on the fly while doing look ahead filtering
              if (key_value_pair != dim_hash_end) {
                // FilterJoin supports only join on unique key columns
                assert(key_value_pair->second->size() == 1);
                dim_indices[join_row_idx++] =
                    key_value_pair->second->at(0).index;
              } else {
                // There's no matched value in the current dimension table
                // then remove the stored matched indices in the prev dimension
                // tables.
                for (size_t prev_filter_id = 0; prev_filter_id < filter_j;
                     prev_filter_id++) {
                  current_idx_temp =
                      prev_dim_indices[prev_filter_id][join_row_idx];
                  prev_dim_indices[prev_filter_id][join_row_idx] =
                      prev_dim_indices[prev_filter_id][indices_length];
                  prev_dim_indices[prev_filter_id][indices_length] =
                      current_idx_temp;
                }
                // Update fact table indices
                current_idx_temp = indices[join_row_idx];
                indices[join_row_idx] = indices[indices_length];
                indices[indices_length--] = current_idx_temp;
              }
            } else {
              // Update the prev dim indices
              for (size_t prev_filter_id = 0; prev_filter_id < filter_j;
                   prev_filter_id++) {
                current_idx_temp =
                    prev_dim_indices[prev_filter_id][join_row_idx];
                prev_dim_indices[prev_filter_id][join_row_idx] =
                    prev_dim_indices[prev_filter_id][indices_length];
                prev_dim_indices[prev_filter_id][indices_length] =
                    current_idx_temp;
              }
              // Update fact table indices
              current_idx_temp = indices[join_row_idx];
              indices[join_row_idx] = indices[indices_length];
              indices[indices_length--] = current_idx_temp;
            }
          }
        }
        // Update the prev dim indices
        for (size_t prev_filter_id = 0; prev_filter_id < filter_j;
             prev_filter_id++) {
          dim_filters_[prev_filter_id].indices_[chunk_i].resize(indices_length +
                                                                1);
        }
        dim_filters_[filter_j].indices_[chunk_i] = std::vector<uint32_t>(
            dim_indices, dim_indices + indices_length + 1);

        lip_indices_[chunk_i].resize(indices_length + 1);
        free(dim_indices);
      }
    });
  }
}

void FilterJoin::ProbeFilters(Task *ctx) {
  int num_chunks =
      fact_fk_cols_[fact_fk_col_names_[0]].chunked_array()->num_chunks();
  batch_size_ =
      std::thread::hardware_concurrency() * options_->get_parallel_factor();

  if (batch_size_ == 0) batch_size_ = num_chunks;
  int num_batches = num_chunks / batch_size_ +
                    1;  // if num_chunks is a multiple of batch_size, we don't
                        // actually want the +1
  if (num_batches == 0) num_batches = 1;

  std::vector<Task *> tasks;
  tasks.reserve(num_batches);

  for (int batch_i = 0; batch_i < num_batches; batch_i++) {
    auto probe_task =
        CreateLambdaTask([this, batch_i, num_chunks](Task *internal) {
          int base_i = batch_i * batch_size_;

          std::vector<Task *> tasks;
          for (int filter_j = 0; filter_j < dim_tables_.size(); ++filter_j) {
            auto probe_task_one_filter = CreateLambdaTask(
                [this, base_i, filter_j, num_chunks](Task *internal) {
                  if (base_i + batch_size_ - 1 < num_chunks) {
                    ProbeFilters(base_i, base_i + batch_size_ - 1, filter_j,
                                 internal);
                  } else {
                    ProbeFilters(base_i, num_chunks - 1, filter_j, internal);
                  }
                });

            tasks.push_back(probe_task_one_filter);
          }
          internal->spawnTask(CreateTaskChain(tasks));
        });

    // Task 2 = update Bloom filter statistics and sort filters accordingly
    auto update_and_sort_task = CreateLambdaTask([this] {
      for (int i = 0; i < dim_filters_.size(); ++i) {
        dim_filters_[i].bloom_filter->update();
      }
      std::sort(dim_filters_.begin(), dim_filters_.end(),
                SortByBloomFilterJoin);
    });

    // Require that Task 2 start only after Task 1 is finished. Each task
    // in tasks probes one batch and then sorts the filters.
    tasks.push_back(CreateTaskChain(probe_task, update_and_sort_task));
  }

  ctx->spawnTask(CreateTaskChain(tasks));
}

void FilterJoin::Initialize(Task *ctx) {
  for (size_t table_idx = 0; table_idx < dim_tables_.size(); table_idx++) {
    auto fact_join_col_name = fact_fk_col_names_[table_idx];
    fact_fk_cols_.emplace(fact_join_col_name, arrow::Datum());
  }

  // Pre-materialized and save fact table fk columns.
  for (size_t table_idx = 0; table_idx < dim_tables_.size(); table_idx++) {
    ctx->spawnLambdaTask([this, table_idx](Task *internal) {
      auto fact_join_col_name = fact_fk_col_names_[table_idx];
      fact_table_.MaterializeColumn(internal, fact_join_col_name,
                                    fact_fk_cols_[fact_join_col_name]);
    });
  }
}

void FilterJoin::execute(Task *ctx) {
  for (auto &result : prev_result_vec_) {
    prev_result_->append(result);
  }
  auto predicates = graph_.get_predicates(0);

  // Loop over the join predicates and store the left/right LazyTables and the
  // left/right join column names
  for (auto &jpred : predicates) {
    auto left_ref = jpred.left_col_;
    auto right_ref = jpred.right_col_;

    fact_fk_col_names_.push_back(left_ref.col_name);
    dim_pk_col_names_.push_back(right_ref.col_name);
    dim_pk_cols_[right_ref.col_name] = arrow::Datum();

    // The previous result prev may contain many LazyTables. Find the
    // LazyTables that we want to join.
    for (size_t lazy_tbl_idx = 0;
         lazy_tbl_idx < prev_result_->lazy_tables_.size(); lazy_tbl_idx++) {
      auto lazy_table = prev_result_->get_table(lazy_tbl_idx);

      if (left_ref.table == lazy_table.table) {
        fact_table_ = lazy_table;  // left table is always the same
        if (lazy_table.indices.kind() != arrow::Datum::NONE)
          fact_indices_ = lazy_table.indices.array()->GetValues<uint32_t>(1, 0);
        else
          fact_indices_ = nullptr;
      } else if (right_ref.table == lazy_table.table) {
        dim_tables_.push_back(lazy_table);
      }
    }
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task *internal) {
        Initialize(internal);
        BuildFilters(internal);
      }),
      CreateLambdaTask([this](Task *internal) {
        // Grab any fact table column so we can pre-compute chunk row offsets.
        for (auto &name : fact_fk_col_names_) {
          fact_fk_cols2_[name] = fact_fk_cols_[name].chunked_array();
        }
        auto fact_col = fact_fk_cols_[fact_fk_col_names_[0]].chunked_array();
        lip_indices_.resize(fact_col->num_chunks());

        for (auto &bf : dim_filters_) {
          bf.indices_.resize(fact_col->num_chunks());
        }
        chunk_row_offsets_.resize(fact_col->num_chunks());
        chunk_row_offsets_[0] = 0;
        for (size_t chunk_idx = 1; chunk_idx < fact_col->num_chunks();
             chunk_idx++) {
          chunk_row_offsets_[chunk_idx] =
              chunk_row_offsets_[chunk_idx - 1] +
              fact_col->chunk(chunk_idx - 1)->length();
        }
        ProbeFilters(internal);
      }),
      CreateLambdaTask([this]() { Finish(); })));
}

void FilterJoin::Finish() {
  arrow::Status status;
  arrow::UInt32Builder fact_indices_builder;
  arrow::UInt16Builder fact_index_chunks_builder;

  std::shared_ptr<arrow::UInt32Array> fact_indices;
  std::shared_ptr<arrow::UInt16Array> fact_index_chunks;

  // Append all of the LIP indices to an ArrayBuilder.
  for (size_t chunk_idx = 0; chunk_idx < lip_indices_.size(); chunk_idx++) {
    status = fact_indices_builder.AppendValues(lip_indices_[chunk_idx]);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    auto temp_chunk_indices =
        (uint16_t *)malloc(sizeof(uint16_t) * lip_indices_[chunk_idx].size());
    std::fill_n(temp_chunk_indices, lip_indices_[chunk_idx].size(), chunk_idx);
    status = fact_index_chunks_builder.AppendValues(
        temp_chunk_indices, lip_indices_[chunk_idx].size());
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    free(temp_chunk_indices);
  }

  std::vector<std::shared_ptr<arrow::UInt32Array>> dim_indices;
  dim_indices.resize(dim_filters_.size());

  for (size_t filter_idx = 0; filter_idx < dim_filters_.size(); filter_idx++) {
    arrow::UInt32Builder dim_indices_builder;

    auto indices = dim_filters_[filter_idx].indices_;
    for (size_t chunk_idx = 0; chunk_idx < indices.size(); chunk_idx++) {
      status = dim_indices_builder.AppendValues(indices[chunk_idx]);
      evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }
    status = dim_indices_builder.Finish(&dim_indices[filter_idx]);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
  }

  // Construct new fact table index array
  status = fact_indices_builder.Finish(&fact_indices);
  evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

  status = fact_index_chunks_builder.Finish(&fact_index_chunks);
  evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

  std::vector<LazyTable> output_lazy_tables;
  // Create a new lazy fact table with the new index array
  output_lazy_tables.emplace_back(fact_table_.table, arrow::Datum(),
                                  fact_indices, fact_index_chunks);
  // Add all dimension tables to the output without changing them.
  for (size_t dim_tbl_idx = 0; dim_tbl_idx < dim_tables_.size();
       dim_tbl_idx++) {
    output_lazy_tables.emplace_back(dim_tables_[dim_tbl_idx].table,
                                    arrow::Datum(), dim_indices[dim_tbl_idx],
                                    arrow::Datum(),
                                    dim_tables_[dim_tbl_idx].hash_table());
  }
  OperatorResult result({output_lazy_tables});
  output_result_->append(std::make_shared<OperatorResult>(result));
}

}  // namespace hustle::operators
