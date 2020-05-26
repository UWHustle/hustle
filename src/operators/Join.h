#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "OperatorResult.h"
#include "JoinGraph.h"
#include "Operator.h"

namespace hustle::operators {

class Join : public Operator {
public:

    /**
     * Construct an Join operator to perform joins on two or more tables.
     *
     * @param prev_result OperatorResult form an upstream operator.
     * @param graph A graph specifying all join predicates
     */
    Join(const std::size_t query_id,
         std::shared_ptr<OperatorResult> prev_result,
         std::shared_ptr<OperatorResult> output_result,
         JoinGraph graph);

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

    // Operator result from an upstream operator
    std::shared_ptr<OperatorResult> prev_result_;
    std::shared_ptr<OperatorResult> output_result_;

    // A graph specifying all join predicates
    JoinGraph graph_;


    std::unordered_map<int64_t, int64_t> hash_table_;

    std::vector<std::vector<int64_t>> new_left_indices_vector;
    std::vector<std::vector<int64_t>> new_right_indices_vector;

    std::vector<arrow::compute::Datum> joined_indices_;

    std::mutex hash_table_mutex_;
    std::mutex join_mutex_;


    /**
     * Build a hash table on a column. It is assumed that the column will be
     * of INT64 type.
     *
     * @param col the column
     * @return a hash table mapping key values to their index location in the
     * table.
     */
    void build_hash_table
        (const std::shared_ptr<arrow::ChunkedArray> &col, Task *ctx);

    /**
     * Perform the probe phase of hash join.
     *
     * @param probe_col The column we want to probe into the hash table
     * @return A pair of index arrays corresponding to rows of the left table
     * that join with rows of the right table.
     */
    void probe_hash_table
        (const std::shared_ptr<arrow::ChunkedArray> &probe_col, Task *ctx);

    /**
     * Perform a single hash join.
     *
     * @return An OperatorResult containing the same LazyTables passed in as
     * prev_result, but now their index arrays are updated, i.e. all indices
     * that did not satisfy the join predicate are not included.
     */
    void hash_join(LazyTable left, std::string left_col, LazyTable right,
                   std::string right_col, Task *ctx);

    /**
     * After performing a single join, we must eliminate rows from other
     * LazyTables in prev_result that do are not included in the join result.
     *
     * @param joined_indices A pair of index arrays corresponding to rows of
     * the left table that join with rows of the right table.
     *
     * @return An OperatorResult containing the same LazyTables passed in as
     * prev_result, but now their index arrays are updated, i.e. all indices
     * that did not satisfy the join predicate are not included.
     */
    std::shared_ptr<OperatorResult> back_propogate_result
        (LazyTable left, LazyTable right,
         std::vector<arrow::compute::Datum> joined_indices);

    void finish_probe();
    void finish();

};

} // namespace hustle

#endif //HUSTLE_JOIN_H