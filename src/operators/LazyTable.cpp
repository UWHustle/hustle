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
        filter = arrow::Datum();
        indices =  arrow::Datum();
    }
    LazyTable::LazyTable(
        std::shared_ptr<Table> table,
        arrow::Datum filter,
        arrow::Datum indices) {

        this->table = table;
        this->filter = filter;
        this->indices = indices;

        materialized_cols_.resize(table->get_num_cols());
        filtered_cols_.reserve(table->get_num_cols());

    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column_by_name
        (std::string col_name) {

        return get_column(table->get_schema()->GetFieldIndex(col_name));
    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column(int i) {

        arrow::Status status;

        if (materialized_cols_[i] != nullptr) {
            return materialized_cols_[i];
        }

        auto col = arrow::Datum(table->get_column(i));

        if (filter.kind() != arrow::Datum::NONE) {
            status = arrow::compute::Filter(col, filter).Value(&col);
        }
        if (indices.kind() != arrow::Datum::NONE) {
            status = arrow::compute::Take(col, indices).Value(&col);
        }

        std::shared_ptr<arrow::ChunkedArray> out_col = col.chunked_array();
        materialized_cols_[i] = out_col;

        return col.chunked_array();
    }

void LazyTable::get_column_by_name(Task *ctx, std::string col_name, arrow::Datum &out) {

    get_column(ctx,table->get_schema()->GetFieldIndex(col_name), out);

}

void LazyTable::get_column(Task* ctx, int i, arrow::Datum& out) {
    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this, i, &out](Task *internal) {

            if (materialized_cols_[i] != nullptr) {
                out.value = materialized_cols_[i];
            } else if (filtered_cols_.count(i) > 0) {
                out.value = filtered_cols_[i];
            } else {
                out = table->get_column(i);
                if (filter.kind() != arrow::Datum::NONE) {
//                    context_.apply_filter(internal, out, filter, out);
//                    filtered_cols_[i] = out.chunked_array();
                }
            }
        }),
        CreateLambdaTask([this, i, &out](Task *internal) {
            if (indices.kind() != arrow::Datum::NONE) {
                context_.apply_indices(internal, out,indices, false, out);
                arrow::Status status;
            }
            materialized_cols_[i] = out.chunked_array();
        })
    ));
}

}