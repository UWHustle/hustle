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

#ifndef HUSTLE_HASH_AGGREGATE_H
#define HUSTLE_HASH_AGGREGATE_H

#include "operators/aggregate/aggregate_const.h"
#include "operators/expression.h"
#include "operators/operator.h"
#include "operators/select/predicate.h"

namespace hustle::operators {

typedef size_t hash_t;
typedef int64_t value_t;
// TODO: Refactor the unordered_maps to use phmap::flat_hash_map.
//  It seems to have great optimization over the hash phrasing.
typedef std::unordered_map<hash_t, value_t> HashMap;
typedef std::unordered_map<hash_t, double> MeanHashMap;
typedef std::unordered_map<hash_t, std::tuple<int, int>> TupleMap;

class HashAggregateStrategy {
 public:
  HashAggregateStrategy() : partitions(0), chunks(0) {}

  HashAggregateStrategy(int partitions, int chunks)
      : partitions(partitions), chunks(chunks) {}

  inline std::tuple<int, int> getChunkID(
      // TODO: We assume each chunk has the same number of data.
      //   It is not always true. The correct algorithm is to
      //      linearly traverse the chunkarray.
      //   Change this algorithm to a generate (and yield the
      //      result each time we calls it).
      int task_id, int num_threads, int num_chunks) {
    assert(task_id >= 0);
    assert(num_threads > 0);
    assert(num_chunks >= 0);

    if (task_id >= num_chunks) {
      return std::make_tuple(-1, -1);
    }

    int M = (num_chunks + num_threads - 1) / num_threads;
    int m = num_chunks / num_threads;
    int FR = num_chunks % num_threads;

    int start = 0;
    int end = 0;
    if (task_id < FR) {
      start = M * task_id;
      end = M * (task_id + 1);
    } else {
      start = M * FR + m * (task_id - FR);
      end = M * FR + m * (task_id + 1 - FR);
    }
    start = start < 0 ? 0 : start;
    end = end > num_chunks ? num_chunks : end;
    return std::make_tuple(start, end);
  }

  inline int suggestedNumTasks() const {
    if (chunks < partitions) {
      return chunks;
    }
    return partitions;
  }

  static inline hash_t HashCombine(hash_t seed, hash_t val) {
    seed ^= val + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }

 private:
  int chunks;
  int partitions;
};

class HashAggregate : public BaseAggregate {
  /**
   * Two-phase aggregate.
   * Procedure:
   *    1. Partition the aggregate columns into different chunks.
   *    2. In parallel, perform the first-phase aggregation.
   *        - For each task, construct a local hash map.
   *        - Fetch the data in its partition and perform aggregation.
   *        - Store the result on local hash map.
   *    3. On master thread, perform the second-phase aggregation.
   *        - Iterates through all the local hash maps constructed in 2 and
   *            aggregate the results on a (bigger) hash map.
   */

  // TODO: Refactor the creation of hash map to a HashAggregateMap class.
  //  This class should design such that:
  //    1. Hook different phase of aggregation into the hash map creation.
  //      Decouple the aggregate kernel from every creation methods.
  //      Ideally, we want to abstract this interface as much as possible.
  //    2. Class template, and take care of polymorphic type from arrow.
  //    3. Customize hash function, with hash-combine supported.
  //    4. Automatic mapping of group-by tuple hash to the row id
  //      on the aggregate column. Hopefully this mapping won't change.

 public:
  HashAggregate(std::size_t query_id, OperatorResult::OpResultPtr prev_result,
                OperatorResult::OpResultPtr output_result,
                std::vector<AggregateReference> aggregate_refs,
                std::vector<ColumnReference> group_by_refs,
                std::vector<OrderByReference> order_by_refs);

  HashAggregate(std::size_t query_id, OperatorResult::OpResultPtr prev_result,
                OperatorResult::OpResultPtr output_result,
                std::vector<AggregateReference> aggregate_refs,
                std::vector<ColumnReference> group_by_refs,
                std::vector<OrderByReference> order_by_refs,
                std::shared_ptr<OperatorOptions> options);

  void Execute(Task* ctx, int32_t flags) override;

  std::string operator_name() override {
    return operator_names.find(OperatorType::HASH_BASED_AGGREGATE)->second;
  }

  inline void set_prev_result(OperatorResult::OpResultPtr prev_result) {
    prev_result_ = std::move(prev_result);
  }

  inline void set_output_result(OperatorResult::OpResultPtr output_result) {
    output_result_ = std::move(output_result);
  }

  inline void set_aggregate_refs(
      std::vector<AggregateReference> aggregate_refs) {
    aggregate_refs_ = std::move(aggregate_refs);
  }

  inline void set_groupby_refs(std::vector<ColumnReference> group_by_refs) {
    group_by_refs_ = std::move(group_by_refs);
  }

  inline void set_orderby_refs(std::vector<OrderByReference> order_by_refs) {
    order_by_refs_ = std::move(order_by_refs);
  }

  void Clear() override {}

 private:
  // Operator result from an upstream operator and output result will be stored
  OperatorResult::OpResultPtr prev_result_, output_result_;
  // The new output table containing the group columns and aggregate columns.
  DBTable::TablePtr output_table_;
  // The output result of each aggregate group (length = num_aggs_)
  std::atomic<int64_t>* aggregate_data_;
  // Hold the aggregate column data (in chunks)
  std::vector<const int64_t*> aggregate_col_data_;

  // References denoting which columns we want to perform an aggregate on
  // and which aggregate to perform.
  std::vector<AggregateReference> aggregate_refs_;
  // References denoting which columns we want to group by and order by
  std::vector<ColumnReference> group_by_refs_;
  std::vector<OrderByReference> order_by_refs_;

  // Map group by column names to the actual group column
  std::vector<arrow::Datum> group_by_cols_;

  arrow::Datum agg_col_;
  std::shared_ptr<arrow::ChunkedArray> filter_;
  // A StructType containing the types of all group by columns
  std::shared_ptr<arrow::DataType> group_type_;
  // We append each aggregate to this after it is computed.
  std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
  // We append each group to this after we compute the aggregate for that
  // group.
  std::shared_ptr<arrow::StructBuilder> group_builder_;
  // Construct the group-by key. Initialized in Dynamic Depth Nested Loop.
  std::vector<std::vector<int>> group_id_vec_;

  // Map group-by column name to group_index in the group_by_refs_ table.
  std::unordered_map<std::string, int> group_by_index_map_;
  std::vector<LazyTable::LazyTablePtr> group_by_tables_;
  LazyTable::LazyTablePtr agg_lazy_table_;

  std::shared_ptr<Expression> expression_;

  // Two phase hashing requires two types of hash tables.
  HashAggregateStrategy strategy;

  // TODO: Refactor the creation of hash map to a HashAggregateMap class.
  // Local hash table that holds the aggregated values for each group.
  // The (temporary) type for key.

  std::vector<HashMap*> count_maps;
  std::vector<HashMap*> value_maps;
  std::vector<TupleMap*> tuple_maps;

  // Map the hash key to (chunk_id, offset).
  TupleMap* global_tuple_map;

  // TODO: Construct a mapping from hash key to group-by column tuples

  // Global hash table handles the second phase of hashing.
  // - HashMap if the aggregate kernel is SUM, COUNT
  // - MeanHashMap if the aggregate kernel is MEAN
  // TODO: Depending on the strategy, the global_map should be polymorphic.
  void* global_map;

  // If a thread wants to insert a group and its aggregate into group_builder_
  // and aggregate_builder_, then it must grab this mutex to ensure that the
  // groups and its aggregates are inserted at the same row.
  std::mutex mutex_;

  void Initialize(Task* internal);

  void ComputeAggregates(Task* internal);

  void Finish(Task* internal);

  /**
   * @return A vector of ArrayBuilders, one for each of the group by columns.
   */
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> CreateGroupBuilderVector();

  /**
   * Construct an ArrayBuilder for the aggregate values.
   *
   * @param kernel The type of aggregate we want to compute
   * @return An ArrayBuilder of the correct type for the aggregate we want
   * to compute.
   */
  std::shared_ptr<arrow::ArrayBuilder> CreateAggregateBuilder(
      AggregateKernel kernel);

  /**
   * Construct the schema for the output table.
   *
   * @param kernel The type of aggregate we want to compute
   * @param agg_col_name The column over which we want to compute the
   * aggregate.
   *
   * @return The schema for the output table.
   */
  std::shared_ptr<arrow::Schema> OutputSchema(AggregateKernel kernel,
                                              const std::string& agg_col_name);

  /**
   * Initialize the hash tables for the first phase of aggregate.
   */
  void InitializeLocalHashTables();

  /**
   * Perform the first aggregate of the two phase hash aggregate.
   * Only aggregate with chunk_id range from [st, ed).
   *
   * @param task_id Task id.
   * @param start_chunk_id The chunk_id to start with.
   * @param end_chunk_id The chunk_id to end with.
   */
  void FirstPhaseAggregateChunks(Task* ctx, size_t task_id, int start_chunk_id,
                                 int end_chunk_id);

  /**
   * Helper method of FirstPhaseAggregateChunks().
   * First phase aggregate on chunk_id.
   *
   * @param task_id Task id.
   * @param chunk_id The chunk_id to work with.
   */
  void FirstPhaseAggregateChunk_(Task* ctx, size_t task_id, int chunk_id);

  /**
   * Perform the second aggregate of the two phase hash aggregate.
   *
   * @param internal The scheduler object.
   */
  void SecondPhaseAggregate(Task* internal);

  void SortResult(std::vector<arrow::Datum>& groups, arrow::Datum& aggregates);
};

}  // namespace hustle::operators

#endif  // HUSTLE_HASH_AGGREGATE_H
