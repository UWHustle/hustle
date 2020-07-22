#ifndef HUSTLE_LIP_H
#define HUSTLE_LIP_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "BloomFilter.h"
#include "OperatorResult.h"
#include "JoinGraph.h"
#include "Operator.h"

namespace hustle::operators {

/**
 * The LIP operator updates the index array of the fact LazyTable in the inputted
 * OperatorResults. After execution, the index array of the fact LazyTable contains
 * the indices of rows that join with all other LazyTables the fact LazyTable
 * was joined with and possibly with some extraneous indices (false positives).
 * The index array of all other LazyTables are unchanged. Filters are unchanged.
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
        std::shared_ptr<OperatorResult> output_result,
        hustle::operators::JoinGraph graph);

    /**
     * Perform LIP on a left-deep join plan.
     *
     * @param ctx A scheduler task
     */
    void execute(Task *ctx) override;

private:
    // Row indices of the fact table that successfully probed all Bloom filters.
    std::vector<std::vector<uint32_t>> lip_indices_;
    const uint32_t* fact_indices_;

    // Number of blocks that are probed (in parallel) before sorting the the filters
    int batch_size_;

    // chunk_row_offsets_[i] = the row index at which the ith block of the fact
    // table starts
    std::vector<int64_t> chunk_row_offsets_;

    // Map of (fact table foreign key col name, fact table foreign key col)
    std::unordered_map<std::string, arrow::Datum> fact_fk_cols_;
    std::unordered_map<std::string, arrow::Datum> dim_pk_cols_;

    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>> fact_fk_cols2_;


    // Primary key cols of all dimension tables.

    // Bloom filters of all dimension tables.
    std::vector<std::shared_ptr<BloomFilter>> dim_filters_;

    std::vector<std::shared_ptr<arrow::ChunkedArray>> dim_col_filters_;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> fact_col_filters_;

    // Dimension (lazy) tables
    std::vector<LazyTable> dim_tables_;
    // Dimension primary key col names
    std::vector<std::string> dim_pk_col_names_;
    // Total number of chunks in each dimension table.
    std::vector<int> dim_join_col_num_chunks_;

    LazyTable fact_table_;
    // Fact table foreign key col names to probe Bloom filters.
    std::vector<std::string> fact_fk_col_names_;

    // Results from upstream operators
    std::vector<std::shared_ptr<OperatorResult>> prev_result_vec_;

    // Results from upstream operators condensed into one object
    std::shared_ptr<OperatorResult> prev_result_;

    // Where the output result will be stored once the operator is executed.
    std::shared_ptr<OperatorResult> output_result_;

    // A graph specifying all join predicates
    JoinGraph graph_;

    /**
     * Initialize auxillary data structures, i.e. precompute elements of
     * chunk_row_offsets_, fetch the foreign key columns of the fact table,
     * and reserve space in lip_indices_.
     */
    void initialize(Task* ctx);

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
};

} // namespace hustle

#endif //HUSTLE_LIP_H
