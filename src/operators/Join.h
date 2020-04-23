#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"
#include "JoinGraph.h"

namespace hustle {
namespace operators {


class Join : public Operator{
public:

    /**
     * Construct an Join operator to perform hash join on two Tables.
     *
     * @param left a ColumnReference containing the left (outer) table and
     * the name of the left join column.
     * @param left_selection the filter returned by an earlier selection on the
     * left table. If no selection was performed, pass in a null Datum.
     * @param right a ColumnReference containing the right (inner) table and
     * the name of the right join column.
     * @param right_selection the filter returned by an earlier selection on the
     * right table. If no selection was performed, pass in a null Datum.
     */
    Join(std::shared_ptr<OperatorResult> prev,
            JoinGraph graph);

    /**
    * Perform a natural join on two tables using hash join.
    *
    * @return A vector of JoinResult.
    */
    std::shared_ptr<OperatorResult> run() override;


private:

    std::shared_ptr<OperatorResult> prev_result_;

    LazyTable left_;
    LazyTable right_;

    std::string left_join_col_name_;
    std::string right_join_col_name_;

    std::unordered_map<int64_t, int64_t> hash_table_;

    std::vector<LazyTable> probe_hash_table
        (std::shared_ptr<arrow::ChunkedArray> probe_col);

    std::unordered_map<int64_t, int64_t> build_hash_table
            (std::shared_ptr<arrow::ChunkedArray> col);
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
