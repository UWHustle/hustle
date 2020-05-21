#ifndef HUSTLE_LAZYTABLE_H
#define HUSTLE_LAZYTABLE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

namespace hustle::operators {

struct ColumnReference {
  std::shared_ptr<Table> table;
  std::string col_name;
};


/**
 * A LazyTable associates a table pointer with a boolean filter and an array
 * of indices. The filter and indices determine which rows of the table are
 * active. A table acquires a filter after a selection is performed on it and
 * indices after a join is performed on it. A LazyTable can be thought of as
 * something between a materialized view and an non-materialized view, since
 * it does not materialize the active rows of the table, but it does contain
 * all the information necessary to materialize the active rows.
 */
class LazyTable {
public:

  LazyTable();

  /**
   * Construct a LazyTable
   *
   * @param table a table
   * @param filter A Boolean ChunkedArray Datum
   * @param indices An INT64 Array Datum
   */
  LazyTable(
      std::shared_ptr<Table> table,
      arrow::compute::Datum filter,
      arrow::compute::Datum indices
  );

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param i column index
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column(int i) const;

  /**
   * Materialize the active rows of one column of the LazyTable. This is
   * achieved by first applying the filter to the column and then applying
   * the indices.
   *
   * @param col_name column name
   * @return A new ChunkedArray column containing only active rows of the
   * column.
   */
  std::shared_ptr<arrow::ChunkedArray> get_column_by_name(
      std::string col_name) const;

  // TODO(nicholas): Should these be private? Should I have accessors
  //  for these instead?
  std::shared_ptr<Table> table;
  arrow::compute::Datum filter; // filters are ChunkedArrays
  arrow::compute::Datum indices; // indices are Arrays
};

}

#endif //HUSTLE_OPERATORRESULT_H
