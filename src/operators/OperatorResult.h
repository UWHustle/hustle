#ifndef HUSTLE_OPERATORRESULT_H
#define HUSTLE_OPERATORRESULT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "LazyTable.h"

namespace hustle::operators {

enum FilterOperator {
    AND,
    OR,
    NONE,
};

class OperatorResult {
public:

    /**
     * Construct an OperatorResult, the type that all operators return. It is
     * simply a collection of LazyTables. Since a LazyTable can be thought
     * of as a "view" for a table, an OperatorResult can be thought of as a
     * collection of "views" across multiple tables.
     *
     * @param lazy_tables a vector of LazyTables
     */
    explicit OperatorResult(std::vector<LazyTable> lazy_tables);

    /**
     * Construct an empty OperatorResult.
     */
    OperatorResult();

    /**
     * Append a new table to the OperatorResult. Internally, we wrap the
     * table parameter in a LazyTable with empty filter and indices before
     * appending.
     *
     * @param table The table to append
     */
    void append(std::shared_ptr<Table> table);

    /**
     * Append a new lazy table to the OperatorResult.
     * @param table The lazy table to append
     */
    void append(LazyTable lazy_table);

    /**
     * Append another OperatorResult to this OperatorResult.
     *
     * @param result Another OperatorResult
     */
    void append(const std::shared_ptr<OperatorResult>& result);

    /**
     * Get the LazyTable at index i
     *
     * @param i
     * @return a LazyTable
     */
    LazyTable get_table(int i);

    /**
     * Get the LazyTable that matches a particular table.
     *
     * @param table
     * @return a LazyTable
     */
    LazyTable get_table(const std::shared_ptr<Table>& table);

    /**
     * Construct a new table from the OperatorResult
     *
     * @param col_refs References to the columns to project in the output table.
     * @return A new table containing all columns specified by col_refs
     */
    std::shared_ptr<Table> materialize(const std::vector<ColumnReference>& col_refs);

    std::vector<LazyTable> lazy_tables_;

    void set_materialized_col(std::shared_ptr<Table> table, int i, std::shared_ptr<arrow::ChunkedArray> col);
};

} // namespace hustle

#endif //HUSTLE_OPERATORRESULT_H
