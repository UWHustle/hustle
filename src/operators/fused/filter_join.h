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

#ifndef HUSTLE_FILTER_JOIN_H
#define HUSTLE_FILTER_JOIN_H

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

struct LookupFilterJoin {
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<phmap::flat_hash_map<int64_t, RecordID>> hash_table;
  std::vector<std::vector<uint32_t>> indices_;
};

static bool SortByBloomFilterJoin(const LookupFilterJoin &lhs,
                                  const LookupFilterJoin &rhs) {
  return BloomFilter::compare(lhs.bloom_filter, rhs.bloom_filter);
}

/**
 * FilterJoin operator is an implementation for operator-fusion of
 * LIP (Look-ahead Information Passing and Join operator). It does the
 * look-ahead filtering and after filtering, it checks the hash and does a join
 * on the fly i.e it stores the matched indexes in the dimension and fact table
 * and also removes the previously matched indices in case of mismatch.
 */
class FilterJoin : public Operator {
 public:
  /**
   * Construct a FilterJoin operator to perform LIP and Join on a left-deep join
   * plan. We assume that the left table in all of the join predicates is the
   * same and call this table the fact table. The right table in all join
   * predicates correspond to dimension tables.
   *
   * @param query_id Query id
   * @param prev_result_vec OperatorResults from upstream operators
   * @param output_result Where the output of the operator will be stored
   * after execution.
   * @param graph A graph specifying all join predicates
   */
  FilterJoin(const std::size_t query_id,
             std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
             OperatorResult::OpResultPtr output_result,
             hustle::operators::JoinGraph graph);

  FilterJoin(const std::size_t query_id,
             std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
             OperatorResult::OpResultPtr output_result,
             hustle::operators::JoinGraph graph,
             std::shared_ptr<OperatorOptions> options);

  /**
   * Perform LIP and JOIN on a left-deep join plan.
   *
   * @param ctx A scheduler task
   */
  void execute(Task *ctx) override;

  void Clear() override {}

 private:
  // Row indices of the fact table that successfully probed all Bloom filters.
  std::vector<std::vector<uint32_t>> lip_indices_;
  const uint32_t *fact_indices_;

  // Number of blocks that are probed (in parallel) before sorting the the
  // filters
  int batch_size_;

  // chunk_row_offsets_[i] = the row index at which the ith block of the fact
  // table starts
  std::vector<int64_t> chunk_row_offsets_;

  // Map of (fact table foreign key col name, fact table foreign key col)
  std::unordered_map<std::string, arrow::Datum> fact_fk_cols_, dim_pk_cols_;

  std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>
      fact_fk_cols2_;

  // Primary key cols of all dimension tables.

  // Bloom filters of all dimension tables.
  std::vector<LookupFilterJoin> dim_filters_;

  // Dimension (lazy) tables
  std::vector<LazyTable> dim_tables_;
  // Dimension primary key col names
  std::vector<std::string> dim_pk_col_names_;

  LazyTable fact_table_;
  // Fact table foreign key col names to probe Bloom filters.
  std::vector<std::string> fact_fk_col_names_;

  // Results from upstream operators
  std::vector<std::shared_ptr<OperatorResult>> prev_result_vec_;

  // prev_result_ - Results from upstream operators condensed into one object
  // output_result_ - Where the output result will be stored once the operator
  // is executed.
  OperatorResult::OpResultPtr prev_result_, output_result_;

  // A graph specifying all join predicates
  JoinGraph graph_;

  /**
   * Initialize auxillary data structures, i.e. precompute elements of
   * chunk_row_offsets_, fetch the foreign key columns of the fact table,
   * and reserve space in lip_indices_.
   */
  void Initialize(Task *ctx);

  /**
   * Build Bloom filters for all dimension tables.
   * @param ctx A scheduler task
   */
  void BuildFilters(Task *ctx);

  /**
   * Probe all Bloom filters
   *
   * @param ctx A scheduler task
   */
  void ProbeFilters(Task *ctx);

  void ProbeFilters(int chunk_start, int chunk_end, int filter_j, Task *ctx);

  /*
   * Create the output result from the raw data computed during execution.
   */
  void Finish();
};

}  // namespace hustle::operators

#endif  // HUSTLE_FILTER_JOIN_H
