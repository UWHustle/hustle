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

arrow::compute::Datum SelectComposite::select(std::shared_ptr<Table> table) {

    return get_filter(table);
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

    Select::Select(
            arrow::compute::CompareOperator compare_operator,
            std::string column_name,
            arrow::compute::Datum column_value) {
        compare_operator_ = compare_operator;
        column_name_ = std::move(column_name);
        column_value_ = std::move(column_value);
    }

    arrow::compute::Datum Select::select(std::shared_ptr<Table>
            table) {

    return get_filter(table);
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

    } // namespace operators
} // namespace hustle
