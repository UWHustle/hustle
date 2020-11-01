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

#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>

#include <string>

#include "operators/operator.h"
#include "operators/utils/operator_result.h"
#include "parallel_hashmap/phmap.h"
#include "storage/block.h"
#include "storage/table.h"

namespace hustle {
namespace operators {

// Types of aggregates we can perform. COUNT is currently not supported.
enum AggregateKernel { SUM, COUNT, MEAN };

/**
 * A reference structure containing all the information needed to perform an
 * aggregate over a column.
 *
 * @param kernel The type of aggregate we want to compute
 * @param agg_name Name of the new aggregate column
 * @param col_ref A reference to the column over which we want to compute the
 * aggregate
 */
struct AggregateReference {
  AggregateKernel kernel;
  std::string agg_name;
  ColumnReference col_ref;
};

/**
 * Group = a set of column values, one for each column in the GROUP BY clause
 *
 * Pseudo code:
 *
 * Initialize an empty output table T
 * Fetch all the unique value of each column in the GROUP BY clause
 *
 * Iterate over all possible groups G:
 *   Get a filter for each column value in G
 *   Logically AND all the filters (1)
 *   Apply the filter to the aggregate column
 *   If the filtered aggregate column has positive length:
 *      Compute the aggregate over the filtered column
 *      Insert the tuple (G, aggregate) into T
 *
 * If the aggregate column appears in the ORDER BY clause:
 *   Sort T with as specified by the ORDER BY clause
 *
 * (1) The resulting filter corresponds to all rows of the table that are part
 * of group G
 *
 * Example:
 *
 * R =
 * group1 | group2 | data
 *   A    |   a    |  1
 *   A    |   a    |  10
 *   A    |   b    |  100
 *   B    |   b    |  1000
 *
 * SELECT group1, group2, sum(data)
 * FROM R
 * GROUP BY group1, group2
 *
 * All possible groups: (A, a), (A, b), (B, a), (B, b)
 *
 * For group (A, a):
 * group1 filter = [1, 1, 1, 0]
 * group2 filter = [1, 1, 0, 0]
 * group (A, a) filter = [1, 1, 0, 0]
 *
 * data after applying group (A, a) filter = [1, 10]
 * result tuple = (A, a, 11)
 *
 * Group (B, a) does not exist in R. The group (B, a) filter is [0, 0, 0, 0],
 * which means the filtered aggregate column with have length 0. Thus, this
 * group will be excluded from the output.
 */
class Aggregate : public Operator {
 public:
  /**
   * Construct an Aggregate operator. Group by and order by clauses are
   * evaluated from left to right. If there is no group by or order by
   * clause, simply pass in an empty vector.
   *
   * Due to limitations in Arrow, we only support sorting in ascending order.
   *
   * Sorting by the aggregate column is a bit hacky. We need to input a
   * ColumnReference for the aggregate column, but the table containing the
   * aggregate column does not actually exist until we execute the Aggregate
   * operator. For now, I let
   *
   * @param prev_result OperatorResult form an upstream operator.
   * @param aggregate_ref vector of AggregateReferences denoting which
   * columns we want to perform an aggregate on and which aggregate to
   * compute.
   * @param group_by_refs vector of ColumnReferences denoting which columns
   * we should group by
   * @param order_by_refs vector of ColumnReferences denoting which columns
   * we should order by.
   */
  Aggregate(const std::size_t query_id,
            std::shared_ptr<OperatorResult> prev_result,
            std::shared_ptr<OperatorResult> output_result,
            std::vector<AggregateReference> aggregate_units,
            std::vector<ColumnReference> group_by_refs,
            std::vector<ColumnReference> order_by_refs);

  Aggregate(const std::size_t query_id,
            std::shared_ptr<OperatorResult> prev_result,
            std::shared_ptr<OperatorResult> output_result,
            std::vector<AggregateReference> aggregate_units,
            std::vector<ColumnReference> group_by_refs,
            std::vector<ColumnReference> order_by_refs,
            std::shared_ptr<OperatorOptions> options);

  /**
   * Compute the aggregate(s) specified by the parameters passed into the
   * constructor.
   *
   * @return OperatorResult containing a single LazyTable corresponding to
   * the new aggregate table (its filter and indices are set empty). Note
   * that unlike other the result of other operators, the returned
   * OperatorResult does not contain any of the LazyTables contained in the
   * prev_result paramter.
   */
  void execute(Task* ctx) override;

 private:
  std::size_t num_aggs_;
  // Operator result from an upstream operator and output result will be stored
  std::shared_ptr<OperatorResult> prev_result_, output_result_;

  // The new output table containing the group columns and aggregate columns.
  std::shared_ptr<DBTable> output_table_;

  std::atomic<int64_t>* aggregate_data_;
  std::vector<const int64_t*> aggregate_col_data_;

  // References denoting which columns we want to perform an aggregate on
  // and which aggregate to perform.
  std::vector<AggregateReference> aggregate_refs_;
  // References denoting which columns we want to group by and order by
  std::vector<ColumnReference> group_by_refs_, order_by_refs_;

  // Map group by column names to the actual group column
  std::vector<arrow::Datum> group_by_cols_;

  phmap::flat_hash_map<int, int> group_agg_index_map_;
  std::vector<arrow::Datum> unique_values_map_;
  // A vector of Arrays containing the unique values of each of the group
  // by columns.
  std::vector<std::shared_ptr<arrow::Array>> unique_values_;

  // A StructType containing the types of all group by columns
  std::shared_ptr<arrow::DataType> group_type_;
  // We append each aggregate to this after it is computed.
  std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
  // We append each group to this after we compute the aggregate for that
  // group.
  std::shared_ptr<arrow::StructBuilder> group_builder_;
  arrow::Datum agg_col_;

  std::vector<std::vector<int>> group_id_vec_;

  std::unordered_map<std::string, int> group_by_index_map_;
  std::vector<LazyTable> group_by_tables_;
  LazyTable agg_lazy_table_;

  // If a thread wants to insert a group and its aggregate into group_builder_
  // and aggregate_builder_, then it must grab this mutex to ensure that the
  // groups and its aggregates are inserted at the same row.
  std::mutex mutex_;

  /**
   * Initialize or pre-compute data members.
   */
  void Initialize(Task* ctx);

  void InitializeGroupByColumn(Task* ctx, std::size_t group_index);

  void InitializeVariables(Task* ctx);

  void InitializeGroupFilters(Task* ctx);

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
   * Construct an ArrayBuilder for the aggregate values.
   *
   * @param kernel The type of aggregate we want to compute
   * @return An ArrayBuilder of the correct type for the aggregate we want
   * to compute.
   */
  std::shared_ptr<arrow::ArrayBuilder> CreateAggregateBuilder(
      AggregateKernel kernel);

  /**
   * @return A vector of ArrayBuilders, one for each of the group by columns.
   */
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> CreateGroupBuilderVector();

  /**
   * Get the unique values of a particular column specified by a
   * ColumnReference.
   *
   * @param group_ref a ColumnReference indicating which column's unique
   * values we want
   * @return the column's unique values as an Array.
   */
  arrow::Datum ComputeUniqueValues(const ColumnReference& group_ref);

  /**
   * Compute the aggregate over a single group. This calls compute_aggregate()
   * after applying a group filter to the aggregate column.
   *
   * @param group_id indices corresponding to values in unique_values_, e.g.
   * passing in group_id = [0, 3, 7] would insert unique_values_[0],
   * unique_values_[3], and unique_values_[7], into group_builder_.
   * @param agg_col aggregate column
   */
  void ComputeGroupAggregate(Task* ctx, std::size_t agg_index,
                             const std::vector<int>& group_id,
                             const arrow::Datum agg_col);

  /**
   * Compute the aggregate over a column.
   *
   * @param kernel The type of aggregate we want to compute
   * @param aggregate_col The column over which we want to compute the
   * aggregate.
   *
   * @return A Scalar Datum containing the aggregate
   */
  arrow::Datum ComputeAggregate(AggregateKernel kernel,
                                const arrow::Datum& aggregate_col);

  /**
   * Loop over all aggregate groups and compute the aggreate for each group.
   * A new task is spawned for each group aggregate to be computed.
   *
   * @param ctx scheduler task
   */
  void ComputeAggregates(Task* ctx);

  /**
   * Sort the output data with respect to each column in the ORDER BY clause.
   * To produce the correct ordering, we must sort the output with respect
   * columns in the ORDER BY clause but in the reverse order in which they
   * appear. For example, if we have ORDER BY R.a, R.b, then we first sort our
   * output with respect to R.b, and then sort with respect to R.a.
   */
  void SortResult(std::vector<arrow::Datum>& groups, arrow::Datum& aggregates);

  void BlockScan(std::vector<const uint32_t*>& group_map, int chunk_i);

  void InsertGroupColumns(std::vector<int> group_id, int agg_index);

  void InsertGroupAggregate(int agg_index);

  /**
   * Create the output result from data computed during operator execution.
   */
  void Finish();
};

}  // namespace operators
}  // namespace hustle

#endif  // HUSTLE_AGGREGATE_H