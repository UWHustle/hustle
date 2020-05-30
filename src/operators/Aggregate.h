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
 * Initialize an empty output table T
 * Fetch all the unique value of each column in the GROUP BY clause
 * If the aggregate column does not appear in the ORDER BY clause
 *   sort the unique values according to the ORDER BY clause
 *
 * Iterate over all possible groups G:
 *   Get a filter for each column value in G
 *   Logically AND all the filters
 *   Apply the filter to the aggregate column
 *   Compute the aggregate over the filtered column
 *   If the aggregate is > 0:
 *      Insert the tuple (G, aggregate) into T
 *
 */
class Aggregate : public Operator {
public:

    /**
     * Construct an Aggregate operator. Group by and order by clauses are
     * evaluated from left to right. If there is no group byor order by
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

    bool sort_aggregate_col_;
    std::vector<arrow::compute::Datum> groups_;
    arrow::compute::Datum aggregates_;

    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>> group_by_cols_;

    std::shared_ptr<arrow::ChunkedArray> empty_filter_;
    // If a thread wants to insert a group and its aggregate into group_builder_
    // and aggregate_builder_, then it must grab this mutex. Otherwise, another
    // thread may insert a different aggregate, associating the group with an
    // incorrect aggregate.
    std::mutex builder_mutex_;

    // The output table's schema
    std::shared_ptr<arrow::Schema> out_schema_;
    // The output table's data.
    std::vector<std::shared_ptr<arrow::ArrayData>> out_table_data_;
    // The new output table containing the group columns and aggregate columns.
    std::shared_ptr<Table> out_table_;


    // Operator result from an upstream operator
    std::shared_ptr<OperatorResult> prev_result_;
    std::shared_ptr<OperatorResult> output_result_;

    // References denoting which columns we want to perform an aggregate on
    // and which aggregate to perform.
    std::vector<AggregateReference> aggregate_refs_;
    // References denoting which columns we want to group by
    std::vector<ColumnReference> group_by_refs_;
    // References denoting which columns we want to group by
    std::vector<ColumnReference> order_by_refs_;

    std::vector<int64_t> tuple_ordering_;

    // We append each aggregate to this after it is computed.
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;

    // A StructType containing the types of all group by columns
    std::shared_ptr<arrow::DataType> group_type_;
    // We append each group to this after we compute the aggregate for that
    // group.
    std::shared_ptr<arrow::StructBuilder> group_builder_;
    // A vector of Arrays containing the unique values of each of the group
    // by columns.
    std::vector<std::shared_ptr<arrow::Array>> all_unique_values_;

    void sort();

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
        AggregateKernels kernel, std::string agg_col_name);

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
     * Compute the aggregate for a single group.
     *
     * @param kernel The type of aggregate we want to compute
     * @param aggregate_col The column over which we want to compute the
     * aggregate.
     * @param group_filter A filter corresponding to all rows in the table
     * that are part of a particular group.
     *
     * @return A Scalar Datum containing the aggregate
     */
    arrow::compute::Datum compute_aggregate(
        AggregateKernels kernel,
        std::shared_ptr<arrow::ChunkedArray> aggregate_col);


    /**
     * Insert a particular group into the group_builder_
     *
     * @param its indices corresponding to values in unique_values_, e.g.
     * passing in its = [0, 3, 7] would insert unique_values_[0],
     * unique_values_[3], and unique_values_[7], into group_builder_.
     */
    void insert_group(std::vector<int> its);

    /**
     * Insert the aggregate of a single group into aggregate_builder_.
     *
     * @param aggregate The aggregate computed for a single group.
     */
    bool insert_group_aggregate(arrow::compute::Datum aggregate, int agg_index);

    /**
     * Get the filter corresponding to a single group across all group by
     * columns. This is achieved by first getting the filters for a single
     * group of a single column and then ANDing all of the filters.
     *
     * @param its indices corresponding to values in unique_values_
     *
     * @return A filter corresponding to rows of the aggregate column
     * associated with the group defined by the its array.
     */
    std::shared_ptr<arrow::ChunkedArray> get_group_filter(std::vector<int> its);

    std::shared_ptr<arrow::Array> get_unique_values(ColumnReference group_ref);

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
        ColumnReference col_ref,
        arrow::compute::Datum value);

    /**
     * @return A vector of ArrayBuilders corresponding to each of the group
     * by columns.
     */
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> get_group_builders();

    void compute_aggregates(Task *ctx);

    void finish();

    void initialize();

    void compute_group_aggregate(int agg_index, std::vector<int> group_id,
                                 std::shared_ptr<arrow::ChunkedArray> agg_col);


};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H