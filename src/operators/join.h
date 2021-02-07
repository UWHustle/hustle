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

#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <arrow/compute/api.h>
#include <utils/parallel_hashmap/phmap.h>

#include <string>

#include "operators/join_graph.h"
#include "operators/operator.h"
#include "operators/utils/operator_result.h"
#include "storage/block.h"
#include "storage/table.h"

namespace hustle::operators {

/**
 * The Join operator updates the index arrays of each LazyTable in the inputted
 * OperatorResults. After execution, the index arrays of each LazyTable contains
 * only the indices of rows that join with all other LazyTables this LazyTable
 * was joined with. Filters are unchanged.
 *
 * See slides 18-27 for an in-depth example:
 * https://docs.google.com/presentation/d/1KlNdwwTy5k-cwlRwY_hRg-AQ9dt3mh_k_MVuIsmyQbQ/edit#slide=id.p
 */
class Join : public Operator {
 public:
  /**
   * Construct an Join operator to perform joins on two or more tables.
   *
   * @param prev_result OperatorResult form an upstream operator.
   * @param graph A graph specifying all join predicates
   */
  Join(const std::size_t query_id,
       std::vector<std::shared_ptr<OperatorResult>> prev_result,
       OperatorResult::OpResultPtr output_result, JoinGraph graph);

  Join(const std::size_t query_id,
       std::vector<std::shared_ptr<OperatorResult>> prev_result,
       OperatorResult::OpResultPtr output_result, JoinGraph graph,
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

  void Clear() override;

  inline void set_prev_result(std::vector<std::shared_ptr<OperatorResult>> prev_result) {
    prev_result_vec_ = prev_result;
  }

  inline void set_output_result(OperatorResult::OpResultPtr output_result) {
    output_result_ = output_result;
  }

  inline void set_join_graph(JoinGraph join_graph) { graph_ = join_graph; }

  void Initialize() {
    prev_result_ = std::make_shared<OperatorResult>();
    joined_indices_.resize(2);
    joined_index_chunks_.resize(2);
  }

 private:
  // lefts_[i] = the left table in the ith join
  // rights_[i] = the right table in the ith join
  std::vector<LazyTable> lefts_, rights_;

  // left_col_names[i] = the left join col name in the ith join
  // right_col_names[i] = the right join col name in the ith join
  std::vector<std::string> left_col_names_, right_col_names_;

  // Results from upstream operators
  std::vector<std::shared_ptr<OperatorResult>> prev_result_vec_;
  // Results from upstream operators condensed into one object
  // Where the output result will be stored once the operator is executed.a
  OperatorResult::OpResultPtr prev_result_, output_result_;

  // A graph specifying all join predicates
  JoinGraph graph_;

  // Hash table for the right table in each join
  std::vector<std::shared_ptr<phmap::flat_hash_map<int64_t, RecordID>>>
      hash_tables_;

  // new_left_indices_vector[i] = the indices of rows joined in chunk i in
  // the left table
  std::vector<std::vector<uint32_t>> new_left_indices_vector_,
      new_right_indices_vector_;
  // new_right_indices_vector[i] = the indices of rows joined in chunk i in
  // the right table
  std::vector<std::vector<uint16_t>> left_index_chunks_vector_,
      right_index_chunks_vector_;

  // It would make much more sense to use an ArrayVector instead of a vector of
  // vectors, since we can make a ChunkedArray out from an ArrayVector. But
  // Arrow's Take function is very inefficient when the indices are in a
  // ChunkedArray. So, we instead use a vector of vectors to store the indices,
  // and then construct an Array from the vector of vectors.

  // joined_indices[0] = new_left_indices_vector stored as an Array
  // joined_indices[1] = new_right_indices_vector stored as an Array
  std::vector<arrow::Datum> joined_indices_, joined_index_chunks_;

  arrow::Datum left_join_col_, right_join_col_;

  LazyTable left_, right_;

  std::unordered_map<DBTable::TablePtr, bool> finished_;

  /**
   * Perform a single hash join.
   *
   * @return An OperatorResult containing the same LazyTables passed in as
   * prev_result, but now their index arrays are updated, i.e. all indices
   * that did not satisfy the join predicate are not included.
   */
  void HashJoin(int join_id, Task *ctx);

  //    void probe_hash_table_block(const std::shared_ptr<arrow::ChunkedArray>
  //    &probe_col, int batch_i, int batch_size,
  //                                std::vector<uint64_t> chunk_row_offsets);
  void BuildHashTable(int join_id,
                      const std::shared_ptr<arrow::ChunkedArray> &col,
                      const std::shared_ptr<arrow::ChunkedArray> &filter,
                      Task *ctx);

  void ProbeHashTable(int join_id,
                      const std::shared_ptr<arrow::ChunkedArray> &probe_col,
                      const arrow::Datum &probe_filter,
                      const arrow::Datum &probe_indices, Task *ctx);
  void ProbeHashTableBlock(
      int join_id, const std::shared_ptr<arrow::ChunkedArray> &probe_col,
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
      LazyTable &left, LazyTable right,
      const std::vector<arrow::Datum> &joined_indices);

  /**
   * probe_hash_table() populates new_left_indices_vector
   * and new_right_indices_vector. This function converts these into Arrow
   * Arrays.
   */
  void FinishProbe(Task *ctx);

  /*
   * Create the output result from the raw data computed during execution.
   */
  void Finish();
};

}  // namespace hustle::operators

#endif  // HUSTLE_JOIN_H