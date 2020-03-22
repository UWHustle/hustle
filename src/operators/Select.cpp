#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>

namespace hustle {
namespace operators {

SelectComposite::SelectComposite(
        std::shared_ptr<SelectOperator> left_child,
        std::shared_ptr<SelectOperator> right_child,
        FilterOperator filter_operator){
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
  filter_operator_ = filter_operator;
}

    std::shared_ptr<Table> SelectComposite::materialize
            (std::vector<std::shared_ptr<Table>> tables,
             arrow::compute::Datum filter) {


        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];

        arrow::SchemaBuilder out_schema_builder;
        status = out_schema_builder.AddSchema(table->get_schema());
        evaluate_status(status, __FUNCTION__, __LINE__);
        std::shared_ptr<arrow::Schema> out_schema;
        auto result = out_schema_builder.Finish();
        status = result.status();
        evaluate_status(status, __FUNCTION__, __LINE__);
        out_schema = result.ValueOrDie();
        auto out_table = std::make_shared<Table>("out", out_schema,
                                                 BLOCK_SIZE);

        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_cols;
        std::shared_ptr<arrow::ChunkedArray> out_col;
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());


        for (int j = 0; j < table->get_num_cols(); j++) {
            status = arrow::compute::Filter(&function_context,
                                            *table->get_column(j),
                                            *filter.chunked_array(),
                                            &out_col);

            evaluate_status(status, __FUNCTION__, __LINE__);
//                out_cols[j] = out_col->array();
            out_table_cols.push_back(out_col);
        }

        std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

        for (int chunk_i=0; chunk_i<out_table_cols[0]->num_chunks();
             chunk_i++) {
            for (auto &col : out_table_cols) {
                out_block_data.push_back(col->chunk(chunk_i)->data());
            }
            out_table->insert_records(out_block_data);
            out_block_data.clear();
        }
        return out_table;
    }

arrow::compute::Datum SelectComposite::get_filter
    (std::shared_ptr<Table> table) {

    arrow::ArrayVector array_vector;
    std::vector<arrow::compute::Datum> block_filters;

    for (int i=0; i<table->get_num_blocks(); i++) {
        auto block = table->get_block(i);
        auto block_filter = get_filter(block);
        array_vector.push_back(block_filter.make_array());
    }

    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(array_vector);
    arrow::compute::Datum out(chunked_filter);
    return out;
}



arrow::compute::Datum SelectComposite::get_filter(std::shared_ptr<Block>
        block) {

    arrow::Status status;

    auto left_child_filter = left_child_->get_filter(block);
    auto right_child_filter = right_child_->get_filter(block);
    // We should never have to check for null children, since
    // SelectComposite is only relevant if it has two children. Otherwise,
    // we would just use Select.

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    arrow::compute::Datum block_filter;

    switch(filter_operator_) {
        case AND: {
            status = arrow::compute::And(&function_context, left_child_filter,
                                         right_child_filter, &block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case OR: {
            status = arrow::compute::Or(&function_context, left_child_filter,
                                        right_child_filter, &block_filter);

            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case NONE: {
            block_filter = left_child_filter;
        }
    }

    return block_filter;

}

    std::shared_ptr<Table> SelectComposite::run_operator
            (std::vector<std::shared_ptr<Table>> tables) {

        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];
        auto out_table = std::make_shared<Table>("out", table->get_schema(),
                                                 BLOCK_SIZE);

        arrow::compute::Datum out_col;
        std::vector<std::shared_ptr<arrow::ArrayData>> out_cols;
        out_cols.reserve(table->get_num_cols());


        for (int i=0; i<table->get_num_blocks(); i++) {
            auto block = table->get_block(i);
            auto block_filter = get_filter(block);

            arrow::compute::FunctionContext function_context(arrow::default_memory_pool());


            for (int j = 0; j < table->get_num_cols(); j++) {

                status = arrow::compute::Filter(&function_context,
                                                block->get_column(j),
                                                block_filter,
                                                &out_col);

                evaluate_status(status, __FUNCTION__, __LINE__);
                out_cols.push_back(out_col.array());
            }
            out_table->insert_records(out_cols);
            out_cols.clear();
        }

        return out_table;

    }

    Select::Select(
            arrow::compute::CompareOperator compare_operator,
            std::string column_name,
            arrow::compute::Datum column_value) {
        compare_operator_ = compare_operator;
        column_name_ = std::move(column_name);
        column_value_ = std::move(column_value);
    }


    arrow::compute::Datum Select::get_filter
            (std::shared_ptr<Block> block) {

        arrow::Status status;

        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::CompareOptions compare_options(compare_operator_);
        arrow::compute::Datum block_filter;

        auto select_col = block->get_column_by_name(column_name_);

        // NOTE: We must fetch filters one block at a time, since the Compare
        // only accepts Array Datum, not ChunkedArray Datum.
        status = arrow::compute::Compare(&function_context,
                                         select_col,
                                         column_value_,
                                         compare_options,
                                         &block_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        return block_filter;
    }

    arrow::compute::Datum Select::get_filter
            (std::shared_ptr<Table> table) {

        arrow::ArrayVector array_vector;
        std::vector<arrow::compute::Datum> block_filters;

        for (int i=0; i<table->get_num_blocks(); i++) {
            auto block = table->get_block(i);
            auto block_filter = get_filter(block);
            array_vector.push_back(block_filter.make_array());
        }

        auto chunked_filter = std::make_shared<arrow::ChunkedArray>(array_vector);
        arrow::compute::Datum out(chunked_filter);
        return out;
    }


    std::shared_ptr<Table> Select::materialize
            (std::vector<std::shared_ptr<Table>> tables,
                    arrow::compute::Datum filter) {


        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];

        arrow::SchemaBuilder out_schema_builder;
        status = out_schema_builder.AddSchema(table->get_schema());
        evaluate_status(status, __FUNCTION__, __LINE__);
        std::shared_ptr<arrow::Schema> out_schema;
        auto result = out_schema_builder.Finish();
        status = result.status();
        evaluate_status(status, __FUNCTION__, __LINE__);
        out_schema = result.ValueOrDie();
        auto out_table = std::make_shared<Table>("out", out_schema,
                                                 BLOCK_SIZE);

        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_cols;
        std::shared_ptr<arrow::ChunkedArray> out_col;
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());


        for (int j = 0; j < table->get_num_cols(); j++) {
            status = arrow::compute::Filter(&function_context,
                                            *table->get_column(j),
                                            *filter.chunked_array(),
                                            &out_col);

            evaluate_status(status, __FUNCTION__, __LINE__);
//                out_cols[j] = out_col->array();
            out_table_cols.push_back(out_col);
        }

        std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

        for (int chunk_i=0; chunk_i<out_table_cols[0]->num_chunks();
             chunk_i++) {
            for (auto &col : out_table_cols) {
                out_block_data.push_back(col->chunk(chunk_i)->data());
            }
            out_table->insert_records(out_block_data);
            out_block_data.clear();
        }
        return out_table;
    }

    std::shared_ptr<Table> Select::run_operator
            (std::vector<std::shared_ptr<Table>> tables) {


        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];

        arrow::SchemaBuilder out_schema_builder;
        status = out_schema_builder.AddSchema(table->get_schema());
        evaluate_status(status, __FUNCTION__, __LINE__);
        std::shared_ptr<arrow::Schema> out_schema;
        auto result = out_schema_builder.Finish();
        status = result.status();
        evaluate_status(status, __FUNCTION__, __LINE__);
        out_schema = result.ValueOrDie();
        auto out_table = std::make_shared<Table>("out", out_schema,
                                                 BLOCK_SIZE);

        std::vector<std::shared_ptr<arrow::ArrayData>> out_cols;
        arrow::compute::Datum out_col;
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

        for (int i=0; i<table->get_num_blocks(); i++) {
            auto block = table->get_block(i);
            auto block_filter = get_filter(block);

            for (int j = 0; j < table->get_num_cols(); j++) {
                status = arrow::compute::Filter(&function_context,
                                                block->get_column(j),
                                                block_filter,
                                                &out_col);

                evaluate_status(status, __FUNCTION__, __LINE__);
//                out_cols[j] = out_col->array();
                out_cols.push_back(out_col.array());
            }
            out_table->insert_records(out_cols);
            out_cols.clear();
        }

        return out_table;
    }

    } // namespace operators
} // namespace hustle
