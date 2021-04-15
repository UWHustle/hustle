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

#ifndef HUSTLE_MULTIWAY_JOIN_H
#define HUSTLE_JOIN_H

#include <arrow/compute/api.h>
#include <utils/parallel_hashmap/phmap.h>

#include <string>

#include "operators/join/join_graph.h"
#include "operators/operator.h"
#include "operators/utils/operator_result.h"
#include "storage/base_block.h"
#include "storage/base_table.h"

namespace hustle::operators {

/**
 * The Join operator updates the index arrays of each LazyTable in the inputted
 * OperatorResults. After execution, the index arrays of each LazyTable contains
 * only the indices of rows that join with all other LazyTables this LazyTable
 * was joined with. Filters are unchanged.
 */
class HashJoin : public Operator {
 public:
  /**
   * Construct an Join operator to perform joins on two or more tables.
   *
   * @param prev_result OperatorResult form an upstream operator.
   * @param predicate join preicate on join column
   */
  HashJoin(const std::size_t query_id,
           std::vector<std::shared_ptr<OperatorResult>> prev_result,
           OperatorResult::OpResultPtr output_result,
           std::shared_ptr<JoinPredicate> predicate);

  HashJoin(const std::size_t query_id,
           std::vector<std::shared_ptr<OperatorResult>> prev_result,
           OperatorResult::OpResultPtr output_result,
           std::shared_ptr<JoinPredicate> predicate,
           std::shared_ptr<OperatorOptions> options);

  /**
   * Perform a natural join on two tables using hash join.
   *
   * @return An OperatorResult containing the same LazyTables passed in as
   * prev_result, but now their index arrays are updated, i.e. all indices
   * that did not satisfy all join predicates specificed in the join graph
   * are not included.
   */
  void execute(Task *ctx) override;

  std::shared_ptr<JoinPredicate> predicate() { return predicate_; }

  void Clear() override {}

 private:
  // Results from upstream operators
  std::vector<OperatorResult::OpResultPtr> prev_result_;
  // Results from upstream operators condensed into one object
  // Where the output result will be stored once the operator is executed.a
  OperatorResult::OpResultPtr output_result_;
  LazyTable left_table_, right_table_;
  arrow::Datum lcol_, rcol_;

  // join predicate
  std::shared_ptr<JoinPredicate> predicate_;

  // Hash table for the join
  std::shared_ptr<phmap::flat_hash_map<int64_t, std::shared_ptr<std::vector<RecordID>>>>
      hash_table_;

  std::vector<std::vector<uint32_t>> left_indices_, right_indices_;
  std::vector<arrow::Datum> joined_indices_;

  void Initialize(Task *ctx);
  /**
   * Perform a single hash join.
   */
  void Join(Task *ctx);

  void BuildHashTable(const std::shared_ptr<arrow::ChunkedArray> &col,
                      const std::shared_ptr<arrow::ChunkedArray> &filter,
                      Task *ctx);

  /**
   * probe_hash_table() populates new_left_indices_vector
   * and new_right_indices_vector. This function converts these into Arrow
   * Arrays.
   */
  void ProbeHashTable(const std::shared_ptr<arrow::ChunkedArray> &probe_col,
                      const arrow::Datum &probe_filter,
                      const arrow::Datum &probe_indices, Task *ctx);
  void ProbeHashTableBlock(
      const std::shared_ptr<arrow::ChunkedArray> &probe_col,
      const std::shared_ptr<arrow::ChunkedArray> &probe_filter, int batch_i,
      int batch_size, std::vector<uint64_t> chunk_row_offsets);

  /**
   * After performing a single join, we must eliminate rows from other
   * LazyTables in prev_result that do are not included in the join result.
   *
   * @param joined_indices A pair of index arrays corresponding to rows of
   * the left table that join with rows of the right table.
   * @return An OperatorResult containing the same LazyTables passed in as
   * prev_result, but now their index arrays are updated, i.e. all indices
   * that did not satisfy the join predicate are not included.
   *
   * e.g. Suppose we are join R with S and rows [0, 1] or R join with rows
   * [2, 3] of S. Further suppose that rows [3] of S join with rows [4] of T.
   * At this point, we know we can exclude one of the rows from the first join
   * on R and S. back_propogate_result() would produce the following index
   * arrays:
   *
   * R S T
   * 1 3 4
   *
   */
  OperatorResult::OpResultPtr BackPropogateResult(
      const std::vector<arrow::Datum> &joined_indices);

  void FinishProbe(Task *ctx);
  /**
   * Create the output result from the raw data
   * computed during execution.
   */
  void Finish();

  inline void ComputeChunkOffsets(
      std::vector<uint64_t> &chunk_offsets,
      const std::shared_ptr<arrow::ChunkedArray> &col) {
    chunk_offsets[0] = 0;
    for (std::size_t i = 1; i < col->num_chunks(); i++) {
      chunk_offsets[i] = chunk_offsets[i - 1] + col->chunk(i - 1)->length();
    }
  }
};

}  // namespace hustle::operators

#endif  // HUSTLE_MULTIWAY_JOIN_H
