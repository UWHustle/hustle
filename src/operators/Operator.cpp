//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>
#include <iostream>
#include "table/util.h"

namespace hustle {
namespace operators {

    LazyTable::LazyTable() {
//        table = std::make_shared<Table>();
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
    }

    OperatorResult::OperatorResult(
            std::vector<LazyTable> units) {

        lazy_tables_ = units;
    }

    OperatorResult::OperatorResult(){

    }

    void OperatorResult::append(std::shared_ptr<Table> table) {
        LazyTable lazy_table(
                table,
                arrow::compute::Datum(),
                arrow::compute::Datum());
        lazy_tables_.insert(lazy_tables_.begin(),lazy_table);
    }

    void OperatorResult::append(std::shared_ptr<OperatorResult> result) {
        for (auto &lazy_table : result->lazy_tables_) {
            lazy_tables_.push_back(lazy_table);
        }
    }

    LazyTable OperatorResult::get_table(int i) {
        return lazy_tables_[i];
    }

    LazyTable OperatorResult::get_table(std::shared_ptr<Table> table) {
        LazyTable result;
        for (auto & lazy_table : lazy_tables_) {
            if (lazy_table.table == table) {
                result = lazy_table;
                break;
            }
        }
        return result;
    }
    
    std::shared_ptr<Table>
    OperatorResult::materialize(std::vector<ColumnReference> col_refs) {

        arrow::Status status;
        arrow::SchemaBuilder schema_builder;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_cols;

        for (auto &col_ref : col_refs) {

            auto table = col_ref.table;
            auto col_name = col_ref.col_name;

            status = schema_builder.AddField(
                    table->get_schema()->GetFieldByName(col_name));
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

            auto col = get_table(table).get_column_by_name(col_name);
            out_cols.push_back(col);
        }

        status = schema_builder.Finish().status();
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        auto out_schema = schema_builder.Finish().ValueOrDie();

        auto out_table = std::make_shared<Table>("out", out_schema,
                                                 BLOCK_SIZE);


        std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

        // TODO(nicholas): Create Table constuctor that accepts a vector
        //  of ChunkedArrays.
        for (int i = 0;
             i < out_cols[0]->num_chunks(); i++) {
            for (auto &col : out_cols) {
                out_block_data.push_back(col->chunk(i)->data());
            }
            out_table->insert_records(out_block_data);
            out_block_data.clear();
        }

        return out_table;
    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column_by_name
        (std::string col_name) {

        return get_column(table->get_schema()->GetFieldIndex(col_name));
    }

    std::shared_ptr<arrow::ChunkedArray> LazyTable::get_column(int i) {

        arrow::Status status;
        auto col = arrow::compute::Datum(table->get_column(i));

        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::shared_ptr<arrow::ChunkedArray> out;

        // TODO(nicholas): Filters are ChunkedArrays while indices are Arrays.
        //  If this changes in the future (it will!), then you will ned to
        //  update these switch blocks.
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


        return col.chunked_array();
    }

}
}