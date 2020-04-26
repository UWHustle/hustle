#ifndef HUSTLE_LAZYTABLE_H
#define HUSTLE_LAZYTABLE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

namespace hustle::operators{

struct ColumnReference {
    std::shared_ptr<Table> table;
    std::string col_name;
};

class LazyTable {

public:

    LazyTable();
    LazyTable(
            std::shared_ptr<Table> table,
            arrow::compute::Datum filter,
            arrow::compute::Datum indices
    );
    std::shared_ptr<arrow::ChunkedArray> get_column(int i);
    std::shared_ptr<arrow::ChunkedArray> get_column_by_name(
            std::string col_name);

    // TODO(nicholas): Should these be private? Should I have accessors
    //  for these instead?
    std::shared_ptr<Table> table;
    arrow::compute::Datum filter; // filters are ChunkedArrays
    arrow::compute::Datum indices; // indicess are Arrays

};

}

#endif //HUSTLE_OPERATOR_H
