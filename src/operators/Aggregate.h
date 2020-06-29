#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include <utils/ContextPool.h>

#include "OperatorResult.h"
#include "Operator.h"

namespace hustle {
namespace operators {

// Types of aggregates we can perform. COUNT is currently not supported.
enum AggregateKernels {
    SUM,
    COUNT,
    MEAN
};

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
    AggregateKernels kernel;
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
    Aggregate(
        const std::size_t query_id,
        std::shared_ptr<OperatorResult> prev_result,
        std::shared_ptr<OperatorResult> output_result,
        std::vector<AggregateReference> aggregate_units,
        std::vector<ColumnReference> group_by_refs,
        std::vector<ColumnReference> order_by_refs);

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
    void execute(Task *ctx) override;



private:

    // Operator result from an upstream operator
    std::shared_ptr<OperatorResult> prev_result_;
    // Where the output result will be stored
    std::shared_ptr<OperatorResult> output_result_;

    // The output table's schema
    std::shared_ptr<arrow::Schema> out_schema_;
    // The new output table containing the group columns and aggregate columns.
    std::shared_ptr<Table> output_table_;
    // The output table's data.
    std::vector<std::shared_ptr<arrow::ArrayData>> output_table_data_;

    // Group columns for the output table.
    std::vector<arrow::Datum> groups_;
    // Aggregate column for the output table.
    arrow::Datum aggregates_;

    // References denoting which columns we want to perform an aggregate on
    // and which aggregate to perform.
    std::vector<AggregateReference> aggregate_refs_;
    // References denoting which columns we want to group by
    std::vector<ColumnReference> group_by_refs_;
    // References denoting which columns we want to group by
    std::vector<ColumnReference> order_by_refs_;

    // Flag indicating whether or not the aggregate column is included in the
    // ORDER BY clause
    bool sort_aggregate_col_;

    // Map group by column names to the actual group column
    std::vector<arrow::Datum> group_by_cols_;

    std::vector<std::shared_ptr<arrow::ChunkedArray>> group_filters_;
    std::vector<std::vector<std::shared_ptr<arrow::ChunkedArray>>> unique_value_filters_;
    std::vector<arrow::Datum> filtered_agg_cols_;
    std::vector<Context> contexts_;
    std::vector<std::vector<Context>> unique_value_filter_contexts_;



    // A vector of Arrays containing the unique values of each of the group
    // by columns.
    std::vector<std::shared_ptr<arrow::Array>> all_unique_values_;

    // A StructType containing the types of all group by columns
    std::shared_ptr<arrow::DataType> group_type_;
    // We append each aggregate to this after it is computed.
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
    // We append each group to this after we compute the aggregate for that
    // group.
    std::shared_ptr<arrow::StructBuilder> group_builder_;
    // If a thread wants to insert a group and its aggregate into group_builder_
    // and aggregate_builder_, then it must grab this mutex to ensure that the
    // groups and its aggregates are inserted at the same row.
    std::mutex builder_mutex_;

    std::mutex unique_value_filters_mutex_;
    arrow::Datum agg_col_;
    LazyTable agg_lazy_table_;
    std::mutex mutex_;
    std::mutex mutex2_;

    std::vector<std::vector<int>> group_id_vec_;
    int num_aggs_;

    std::unordered_map<std::string, int> group_by_col_names_to_index_;
    std::vector<LazyTable> group_by_tables_;

    ContextPool context_pool_;

    /**
     * Initialize or pre-compute data members.
     */
    void initialize(Task* ctx);

    /**
     * Construct the schema for the output table.
     *
     * @param kernel The type of aggregate we want to compute
     * @param agg_col_name The column over which we want to compute the
     * aggregate.
     *
     * @return The schema for the output table.
     */
    std::shared_ptr<arrow::Schema> get_output_schema(
        AggregateKernels kernel, const std::string& agg_col_name);

    /**
     * Construct an ArrayBuilder for the aggregate values.
     *
     * @param kernel The type of aggregate we want to compute
     * @return An ArrayBuilder of the correct type for the aggregate we want
     * to compute.
     */
    std::shared_ptr<arrow::ArrayBuilder> get_aggregate_builder(
        AggregateKernels kernel);

    /**
     * @return A vector of ArrayBuilders, one for each of the group by columns.
     */
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> get_group_builders();

    /**
     * Get the unique values of a particular column specified by a
     * ColumnReference.
     *
     * @param group_ref a ColumnReference indicating which column's unique
     * values we want
     * @return the column's unique values as an Array.
     */
    arrow::Datum get_unique_values(const ColumnReference& group_ref);

    /**
     * Loop over all aggregate groups and compute the aggreate for each group.
     * A new task is spawned for each group aggregate to be computed.
     *
     * @param ctx scheduler task
     */
    void compute_aggregates(Task *ctx);

    /**
     * Get the filter corresponding to a single group across all group by
     * columns. This is achieved by first getting the filters for a single
     * group of a single column (by calling get_unique_value_filter()) and then
     * ANDing all of the filters.
     *
     * @param its indices corresponding to values in unique_values_
     *
     * @return A filter corresponding to rows of the aggregate column
     * associated with the group defined by the its array.
     */
    void get_group_filter(Task* ctx, int agg_index, const std::vector<int>& its);

    /**
     * Get the filter corresponding to a single group of a single column.
     *
     * @param col_ref One of the group by ColumnReferences
     * @param value The value of a single group in the column referenced by
     * col_ref.
     *
     * @return A filter corresponding to all rows of col_ref equal to value,
     * i.e. the filter corresponding to a single group of col_ref.
     */
    void get_unique_value_filter(
        Task* ctx,
        int agg_index,
        int field_i,
        const ColumnReference& col_ref,
        const arrow::Datum& value,
        std::shared_ptr<arrow::ChunkedArray>& out);

    /**
     * Compute the aggregate over a single group. This calls compute_aggregate()
     * after applying a group filter to the aggregate column.
     *
     * @param group_id indices corresponding to values in unique_values_, e.g.
     * passing in group_id = [0, 3, 7] would insert unique_values_[0],
     * unique_values_[3], and unique_values_[7], into group_builder_.
     * @param agg_col aggregate column
     */
    void compute_group_aggregate(Task* ctx, int agg_index, const std::vector<int>& group_id,
                                 const arrow::Datum& agg_col);

    /**
     * Compute the aggregate over a column.
     *
     * @param kernel The type of aggregate we want to compute
     * @param aggregate_col The column over which we want to compute the
     * aggregate.
     *
     * @return A Scalar Datum containing the aggregate
     */
    arrow::Datum compute_aggregate(
        AggregateKernels kernel,
        const arrow::Datum& aggregate_col);

    /**
     * Insert a particular group into the group_builder_
     *
     * @param group_id indices corresponding to values in unique_values_, e.g.
     * passing in group_id = [0, 3, 7] would insert unique_values_[0],
     * unique_values_[3], and unique_values_[7], into group_builder_.
     */
    void insert_group(std::vector<int> grooup_id);

    /**
     * Insert the aggregate of a single group into aggregate_builder_.
     *
     * @param aggregate The aggregate computed for a single group.
     */
    void insert_group_aggregate(const arrow::Datum& aggregate);

    /**
     * Create the output result from data computed during operator execution.
     */
    void finish();

    /**
     * Sort the output data with respect to each column in the ORDER BY clause.
     * To produce the correct ordering, we must sort the output with respect
     * columns in the ORDER BY clause but in the reverse order in which they appear.
     * For example, if we have ORDER BY R.a, R.b, then we first sort our output
     * with respect to R.b, and then sort with respect to R.a.
     */
    void sort();


    void initialize_variables(Task *ctx);

    void initialize_group_by_column(Task *ctx, int i);

    void initialize_agg_col(Task *ctx);
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H