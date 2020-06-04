//
// Created by Nicholas Corrado on 4/5/20.
//

#include "OperatorResult.h"

#include <utility>
#include <iostream>
#include "table/util.h"
#include "LazyTable.h"
#include "utils/arrow_compute_wrappers.h"

namespace hustle::operators{

    LazyTable::LazyTable() {
        filter = arrow::compute::Datum();
        indices =  arrow::compute::Datum();
    }
    LazyTable::LazyTable(
        std::shared_ptr<Table> table,
        arrow::compute::Datum filter,
        arrow::compute::Datum indices) {

        this->table = table;
        this->filter = filter;
        this->indices = indices;

        materialized_cols_.reserve(table->get_num_cols());
    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column_by_name
        (std::string col_name) {

        return get_column(table->get_schema()->GetFieldIndex(col_name));
    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column(int i) {

        if (materialized_cols_.count(i) > 0) {
            return materialized_cols_[i];
        }

        auto col = arrow::compute::Datum(table->get_column(i));

        if (filter.kind() != arrow::compute::Datum::NONE) {
            apply_filter(col, filter, &col);
        }
        if (indices.kind() != arrow::compute::Datum::NONE) {
            apply_indices(col,indices, &col);
        }

        std::shared_ptr<arrow::ChunkedArray> out_col = col.chunked_array();
        materialized_cols_.emplace(i, out_col);

        return out_col;
    }


}