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

class LIP : public Operator {
public:

    /**
     * Construct an Join operator to perform joins on two or more tables.
     *
     * @param prev_result OperatorResult form an upstream operator.
     * @param graph A graph specifying all join predicates
     */
    LIP(const std::size_t query_id,
        std::vector<std::shared_ptr<OperatorResult>> prev_result,
        std::shared_ptr<OperatorResult> output_result,
        hustle::operators::JoinGraph graph);

    /**
    * Perform a natural join on two tables using hash join.
    *
    * @return An OperatorResult containing the same LazyTables passed in as
     * prev_result, but now their index arrays are updated, i.e. all indices
     * that did not satisfy all join predicates specificed in the join graph
     * are not included.
    */
    void execute(Task *ctx) override;

private:
    std::vector<std::vector<int64_t>> lip_indices_;

    int batch_size_;
    std::vector<int64_t> chunk_row_offsets_;

    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>> fact_fk_cols_;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> dim_pk_cols_;

    std::vector<std::shared_ptr<BloomFilter>> dim_filters_;

    std::vector<LazyTable> dim_tables_;
    std::vector<std::string> dim_join_col_names_;
    std::vector<int> dim_join_col_num_chunks_;

    LazyTable fact_table_;
    std::vector<std::string> fact_join_col_names_;

    arrow::ArrayVector fact_filter_vec_;

    std::vector<std::shared_ptr<OperatorResult>> prev_result_vec_;

    // Operator result from an upstream operator
    std::shared_ptr<OperatorResult> prev_result_;
    std::shared_ptr<OperatorResult> output_result_;

    // A graph specifying all join predicates
    JoinGraph graph_;

    std::mutex hash_table_mutex_;
    std::mutex join_mutex_;

    void finish();

    void build_filters(Task *ctx);

    void probe_filters(Task *ctx);

    std::mutex build_mutex_;

    void probe_filters(int chunk_i);

    void probe_filters(Task *ctx, int chunk_i);
};

} // namespace hustle

#endif //HUSTLE_LIP_H
