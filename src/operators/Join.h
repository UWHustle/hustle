#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

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
    Join(ColumnReference left,
               std::shared_ptr<OperatorResult> left_selection,
               ColumnReference right,
               std::shared_ptr<OperatorResult> right_selection);

    /**
    * Perform a natural join on two tables using hash join.
    *
    * @return A vector of JoinResult.
    */
    std::vector<OperatorResultUnit> hash_join();
    std::shared_ptr<OperatorResult> run() override;


private:

    arrow::compute::Datum left_filter_;
    arrow::compute::Datum right_filter_;

    std::shared_ptr<arrow::ChunkedArray> left_join_col_;
    std::shared_ptr<arrow::ChunkedArray> right_join_col_;

    arrow::compute::Datum left_selection_;
    arrow::compute::Datum right_selection_;

    //TODO(nicholas): a better name?
    std::vector<OperatorResultUnit> left_join_result_;
    std::vector<OperatorResultUnit> right_join_result_;

    std::shared_ptr<Table> left_table_;
    std::shared_ptr<Table> right_table_;

    std::string left_join_col_name_;
    std::string right_join_col_name_;

    std::shared_ptr<arrow::Array> left_indices_;
    std::shared_ptr<arrow::Array> right_indices_;

    std::unordered_map<int64_t, int64_t> hash_table_;

    std::vector<OperatorResultUnit> hash_join(
            std::vector<OperatorResultUnit>&,
            const std::shared_ptr<Table>& right_table);
    std::vector<OperatorResultUnit> hash_join(
            const std::shared_ptr<Table>& left_table,
            const std::shared_ptr<Table>& right_table);

    std::vector<OperatorResultUnit> probe_hash_table
        (std::shared_ptr<arrow::ChunkedArray> probe_col);
        std::vector<OperatorResultUnit> probe_hash_table_2
                (std::shared_ptr<arrow::ChunkedArray> probe_col);

    std::unordered_map<int64_t, int64_t> build_hash_table
            (std::shared_ptr<arrow::ChunkedArray> col);
    std::shared_ptr<arrow::ChunkedArray> apply_selection
            (std::shared_ptr<arrow::ChunkedArray> col, arrow::compute::Datum
            selection);

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
