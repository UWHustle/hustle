#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

class JoinOperator : public Operator {

    virtual arrow::compute::Datum get_indices() = 0;
};

class Join : public Operator{
public:
    Join(std::string left_column_name, std::string right_column_name);

    // TODO(nicholas): These function are not implemented.
    std::shared_ptr<Table> run_operator
    (std::vector<std::shared_ptr<Table>> table) override;

    /**
    * Perform a natural join on two tables using hash join. Projections are not
    * yet supported; all columns from both tables will be returned in the
    * resulting table (excluding the duplicate join column).
    *
    * @param left_table The table that will probe the hash table
    * @param right_table The table for which a hash table is built
    * @return A new table containing the results of the join
    */

//    Interface Joinable;

    std::vector<SelectionReference> hash_join(
            const std::shared_ptr<Table>& left_table,
            const arrow::compute::Datum& left_selection,
            const std::shared_ptr<Table>& right_table,
            const arrow::compute::Datum& right_selection);

    std::vector<SelectionReference> hash_join(
            const std::vector<SelectionReference>& left_table,
            const std::shared_ptr<Table>& right_table,
            const arrow::compute::Datum& right_selection);

    arrow::compute::Datum get_left_indices();
    arrow::compute::Datum get_right_indices();
    arrow::compute::Datum get_indices_for_table(
            const std::shared_ptr<Table> &other);

private:
    std::vector<SelectionReference> left_;
    std::shared_ptr<Table> left_table_;
    std::shared_ptr<Table> right_table_;

    std::string left_join_column_name_;
    std::string right_join_column_name_;

    std::shared_ptr<arrow::Array> left_indices_;
    std::shared_ptr<arrow::Array> right_indices_;

    std::unordered_map<int64_t, int64_t> hash_table_;

    std::vector<SelectionReference> probe_hash_table
            (std::shared_ptr<arrow::ChunkedArray> probe_col);
        std::vector<SelectionReference> probe_hash_table
                (std::shared_ptr<arrow::ChunkedArray> probe_col, int
                probe_col_index);
    std::unordered_map<int64_t, int64_t> build_hash_table
            (std::shared_ptr<arrow::ChunkedArray> col);
    std::shared_ptr<arrow::ChunkedArray> apply_selection
            (std::shared_ptr<arrow::ChunkedArray> col, arrow::compute::Datum
            selection);



};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
