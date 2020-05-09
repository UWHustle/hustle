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
    
class Join : public Operator{
public:

     /**
      * Construct an Join operator to perform joins on two or more tables.
      *
      * @param prev_result OperatorResult form an upstream operator.
      * @param graph A graph specifying all join predicates
      */
    Join(std::shared_ptr<OperatorResult> prev_result,
            JoinGraph graph);

    /**
    * Perform a natural join on two tables using hash join.
    *
    * @return An OperatorResult containing the same LazyTables passed in as
     * prev_result, but now their index arrays are updated, i.e. all indices
     * that did not satisfy all join predicates specificed in the join graph
     * are not included.
    */
    std::shared_ptr<OperatorResult> run() override;


private:

    // Operator result from an upstream operator
    std::shared_ptr<OperatorResult> prev_result_;
    // A graph specifying all join predicates
    JoinGraph graph_;

    // Left (outer) table
    LazyTable left_;
    // Right (inner) table
    LazyTable right_;

    std::string left_join_col_name_;
    std::string right_join_col_name_;

    std::unordered_map<int64_t, int64_t> hash_table_;

    /**
     * Build a hash table on a column. It is assumed that the column will be
     * of INT64 type.
     *
     * @param col the column
     * @return a hash table mapping key values to their index location in the
     * table.
     */
    std::unordered_map<int64_t, int64_t> build_hash_table
            (const std::shared_ptr<arrow::ChunkedArray>& col);

    /**
     * Perform the probe phase of hash join.
     *
     * @param probe_col The column we want to probe into the hash table
     * @return A pair of index arrays corresponding to rows of the left table
     * that join with rows of the right table.
     */
    std::vector<arrow::compute::Datum> probe_hash_table
        (const std::shared_ptr<arrow::ChunkedArray>& probe_col);

    /**
     * Perform a single hash join.
     *
     * @return An OperatorResult containing the same LazyTables passed in as
     * prev_result, but now their index arrays are updated, i.e. all indices
     * that did not satisfy the join predicate are not included.
     */
    std::shared_ptr<OperatorResult> hash_join();

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
    (std::vector<arrow::compute::Datum> joined_indices);

};

} // namespace hustle

#endif //HUSTLE_JOIN_H
