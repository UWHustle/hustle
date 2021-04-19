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

#ifndef HUSTLE_LIP_H
#define HUSTLE_LIP_H

#include <arrow/compute/api.h>

#include <string>

#include "operators/join/join_graph.h"
#include "operators/operator.h"
#include "operators/utils/operator_result.h"
#include "storage/block.h"
#include "storage/table.h"
#include "utils/bloom_filter.h"
#include "utils/histogram.h"

namespace hustle::operators {

struct LookupFilter {
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<phmap::flat_hash_map<int64_t, std::shared_ptr<std::vector<RecordID>>>> hash_table;
};

static bool SortByBloomFilter(const LookupFilter &lhs,
                              const LookupFilter &rhs) {
  return BloomFilter::compare(lhs.bloom_filter, rhs.bloom_filter);
}

/**
 * The LIP operator updates the index array of the fact LazyTable in the
 * inputted OperatorResults. After execution, the index array of the fact
 * LazyTable contains the indices of rows that join with all other LazyTables
 * the fact LazyTable was joined with and possibly with some extraneous indices
 * (false positives). The index array of all other LazyTables are unchanged.
 * Filters are unchanged.
 */
class LIP : public Operator {
 public:
  /**
   * Construct a LIP operator to perform LIP on a left-deep join plan. We
   * assume that the left table in all of the join predicates is the same and
   * call this table the fact table. The right table in all join predicates
   * correspond to dimension tables.
   *
   * @param query_id Query id
   * @param prev_result_vec OperatorResults from upstream operators
   * @param output_result Where the output of the operator will be stored
   * after execution.
   * @param graph A graph specifying all join predicates
   */
  LIP(const std::size_t query_id,
      std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
      OperatorResult::OpResultPtr output_result,
      hustle::operators::JoinGraph graph);

  /**
   * Perform LIP on a left-deep join plan.
   *
   * @param ctx A scheduler task
   */
  void Execute(Task *ctx, int32_t flags) override;

    std::string operator_name() override {
        return operator_names.find(OperatorType::LIP_OP)->second;
    }

  void Clear() override {}

 private:
  std::unordered_map<std::string, std::vector<std::vector<int64_t>>>
      out_fk_cols_;

  // Row indices of the fact table that successfully probed all Bloom filters.
  std::vector<uint32_t *> lip_indices_raw_;
  std::vector<std::vector<uint32_t>> lip_indices_;
  std::vector<std::vector<uint16_t>> lip_index_chunks_;

  const uint32_t *fact_indices_;

  // Number of blocks that are probed (in parallel) before sorting the the
  // filters
  int batch_size_;

  // chunk_row_offsets_[i] = the row index at which the ith block of the fact
  // table starts
  std::vector<int64_t> chunk_row_offsets_;

  // Map of (fact table foreign key col name, fact table foreign key col)
  std::unordered_map<std::string, arrow::Datum> fact_fk_cols_;
  std::unordered_map<std::string, arrow::Datum> dim_pk_cols_;

  std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>
      fact_fk_cols2_;

  // Primary key cols of all dimension tables.

  // Bloom filters of all dimension tables.
  std::vector<LookupFilter> dim_filters_;

  std::vector<std::shared_ptr<arrow::ChunkedArray>> dim_col_filters_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> fact_col_filters_;

  // Dimension (lazy) tables
  std::vector<LazyTable::LazyTablePtr> dim_tables_;
  // Dimension primary key col names
  std::vector<std::string> dim_pk_col_names_;
  // Total number of chunks in each dimension table.
  std::vector<int> dim_join_col_num_chunks_;

  LazyTable::LazyTablePtr fact_table_;
  // Fact table foreign key col names to probe Bloom filters.
  std::vector<std::string> fact_fk_col_names_;

  // Results from upstream operators
  std::vector<std::shared_ptr<OperatorResult>> prev_result_vec_;

  // Results from upstream operators condensed into one object
  OperatorResult::OpResultPtr prev_result_;

  // Where the output result will be stored once the operator is executed.
  OperatorResult::OpResultPtr output_result_;

  // A graph specifying all join predicates
  JoinGraph graph_;

  /**
   * Initialize auxillary data structures, i.e. precompute elements of
   * chunk_row_offsets_, fetch the foreign key columns of the fact table,
   * and reserve space in lip_indices_.
   */
  void initialize(Task *ctx);

  /**
   * Build Bloom filters for all dimension tables.
   * @param ctx A scheduler task
   */
  void build_filters(Task *ctx);

  /**
   * Probe all Bloom filters
   *
   * @param ctx A scheduler task
   */
  void probe_filters(Task *ctx);

  /**
   * Probe all Bloom filters with one fact table block/chunk.
   * @param ctx A scheduler task
   * @param chunk_i Index of the block/chunk to be probed.
   */
  void probe_filters(int chunk_i);

  /*
   * Create the output result from the raw data computed during execution.
   */
  void finish();

  void probe_filters2(int chunk_i);

  //    void probe_filters(int chunk_start, int chunk_end, int filter_j);

  void probe_filters(int chunk_start, int chunk_end, int filter_j, Task *ctx);

  void probe_all_filters(int chunk_start, int chunk_end, Task *ctx);
};

}  // namespace hustle::operators

#endif  // HUSTLE_LIP_H
