#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

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
 * If the aggregate column does not appear in the ORDER BY clause:
 *   Sort the unique values as specified by the ORDER BY clause
 *
 * Iterate over all possible groups G:
 *   Get a filter for each column value in G
 *   Logically AND all the filters
 *   Apply the filter to the aggregate column
 *   Compute the aggregate over the filtered column
 *   If the aggregate is > 0:
 *      Insert the tuple (G, aggregate) into T
 *
 * If the aggregate column appears in the ORDER BY clause:
 *   Sort T with as specified by the ORDER BY clause
 *
 * If the aggregate column does not appear in the ORDER BY clause, then by
 * sorting unique values prior to computing any group aggregates, we assure
 * that the order in which we iterate over groups G is the order in which the
 * tuples should appear in the output table.
 *
 * If the aggregate column appears in the ORDER BY clause, of course we must
 * sort only after all group aggregates have been computed. Then we sort the
 * columns of T with respect to columns in the ORDER BY clause but in the
 * reverse order in which they appear. 
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
    std::shared_ptr<Table> out_table_;
    // The output table's data.
    std::vector<std::shared_ptr<arrow::ArrayData>> out_table_data_;

    // Group columns for the output table.
    std::vector<arrow::compute::Datum> groups_;
    // Aggregate column for the output table.
    arrow::compute::Datum aggregates_;

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
    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>> group_by_cols_;

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

    // In a multithreaded setting, we cannot guarantee that aggregates are computed
    // and inserted into aggregate_builder_ in the correct order even though we
    // assign single-group aggregation tasks in the correct order. So, we assign
    // each task an index (see agg_index in compute_aggregates()) which is equal
    // to the index at which the aggregate resides when the output is correctly
    // ordered. When an aggregate is inserted into aggregate_builder_, we also
    // append it's agg_index to tuple_ordering_. So, tuple_ordering_ maps the
    // aggregate's current index to its correctly sorted index. For example,
    // say we have two groups A and B with agg_index 0 and 1, respectively. If
    // B's aggregate is computed before A's, then our output looks like
    //
    // B agg_B
    // A agg_A
    //
    // and tuple_ordering = {1 , 0}. Sorting our output by the indices in
    // tuple_ordering_ gives us the correct result:
    //
    // A agg_A
    // B agg_B
    std::vector<int64_t> tuple_ordering_;


    /**
     * Initialize or pre-compute data members.
     */
    void initialize();

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
    arrow::compute::Datum get_unique_values(const ColumnReference& group_ref);

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
    arrow::compute::Datum get_group_filter(std::vector<int> its);

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
    std::shared_ptr<arrow::ChunkedArray> get_unique_value_filter(
        const ColumnReference& col_ref,
        arrow::compute::Datum value);

    /**
     * Compute the aggregate over a single group. This calls compute_aggregate()
     * after applying a group filter to the aggregate column.
     *
     * @param agg_index The aggregate's sorted position in the final output
     * @param group_id indices corresponding to values in unique_values_, e.g.
     * passing in group_id = [0, 3, 7] would insert unique_values_[0],
     * unique_values_[3], and unique_values_[7], into group_builder_.
     * @param agg_col aggregate column
     */
    void compute_group_aggregate(int agg_index, const std::vector<int>& group_id,
                                 arrow::compute::Datum agg_col);

    /**
     * Compute the aggregate over a column.
     *
     * @param kernel The type of aggregate we want to compute
     * @param aggregate_col The column over which we want to compute the
     * aggregate.
     *
     * @return A Scalar Datum containing the aggregate
     */
    arrow::compute::Datum compute_aggregate(
        AggregateKernels kernel,
        const arrow::compute::Datum& aggregate_col);

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
    bool insert_group_aggregate(const arrow::compute::Datum& aggregate, int agg_index);

    /**
     * Create the output result from data computed during operator execution.
     */
    void finish();

    /**
     * If the aggregate column appears in the ORDER BY clause, we must sort
     * after all group aggregates have been computed. This sorts the output data
     * with respect to each column in the ORDER BY clause, but in the reverse
     * order in which they appear.
     */
    void sort();


};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H