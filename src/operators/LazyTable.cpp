//
// Created by Nicholas Corrado on 4/5/20.
//

#include "OperatorResult.h"

#include <utility>
#include <iostream>
#include "table/util.h"
#include "LazyTable.h"

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

        arrow::Status status;
        auto col = arrow::compute::Datum(table->get_column(i));

        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::shared_ptr<arrow::ChunkedArray> out;

        // Filters are ChunkedArrays, indices are Arrays.
        switch(filter.kind()) {
            case arrow::compute::Datum::NONE: {
                break;
            }
            case arrow::compute::Datum::CHUNKED_ARRAY: {
                arrow::compute::FilterOptions filter_options;
                status = arrow::compute::Filter(&function_context,
                                                col,
                                                filter.chunked_array(),
                                                filter_options,
                                                &col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
            default: {
                std::cerr << "LazyTable filter is not ChunkedArray" <<
                          std::endl;
            }
        }

        switch(indices.kind()) {
            case arrow::compute::Datum::NONE: {
                break;
            }
            case arrow::compute::Datum::ARRAY: {
                arrow::compute::TakeOptions take_options;
                std::shared_ptr<arrow::ChunkedArray> chunked_col;
                status = arrow::compute::Take(&function_context,
                                              *col.chunked_array(),
                                              *indices.make_array(),
                                              take_options,
                                              &chunked_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                col = arrow::compute::Datum(chunked_col);
                break;
            }
            default: {
                std::cerr << "LazyTable indices is not Array" <<
                          std::endl;
            }
        }

        std::shared_ptr<arrow::ChunkedArray> out_col = col.chunked_array();
        materialized_cols_.emplace(i, out_col);

        return out_col;
    }

}