#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>

#define BLOCK_SIZE 1024

namespace hustle {
namespace operators {

//SelectComposite::SelectComposite(
//        arrow::compute::CompareOperator compare_operator,
//        std::string column_name,
//        arrow::compute::Datum column_value,
//        std::shared_ptr<Operator> left_child,
//        std::shared_ptr<Operator> right_child,
//        FilterOperator filter_operator) {
//  compare_operator_ = compare_operator;
//  column_name_ = std::move(column_name);
//  column_value_ = std::move(column_value);
//  left_child_ = left_child;
//  right_child_ = right_child;
//  filter_operator_ = filter_operator;
//}

Select::Select(
        arrow::compute::CompareOperator compare_operator,
        std::string column_name,
        arrow::compute::Datum column_value) {
  compare_operator_ = compare_operator;
  column_name_ = std::move(column_name);
  column_value_ = std::move(column_value);
}


arrow::compute::Datum* Select::get_filter
(std::shared_ptr<Block> block) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(compare_operator_);
    auto* block_filter = new arrow::compute::Datum();

    auto select_col = block->get_column_by_name(column_name_);
    status = arrow::compute::Compare(&function_context,
                                        select_col,
                                        column_value_,
                                        compare_options,
                                        block_filter);
    evaluate_status(status, __FUNCTION__, __LINE__);

    return block_filter;
}


std::shared_ptr<arrow::ChunkedArray> Select::get_filter
        (std::shared_ptr<Table> table) {

    // TODO(nicholas): Arrow currenlty doesn't supported Compare with
    //  ChunkedArray, so we just run COmpare on each block individually and
    //  combine the results into one ChunkedArray ourselves.
    arrow::ArrayVector filter_array_vector;

    for (int i=0; i<table->get_num_blocks(); i++) {
        auto block = table->get_block(i);
        auto* block_filter = get_filter(block);
        filter_array_vector.push_back(block_filter->make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_array_vector);

}

//    arrow::Status status;
//
//    std::shared_ptr<arrow::compute::Datum> left_child_filter;
//    std::shared_ptr<arrow::compute::Datum> right_child_filter;
//    if (left_child_ != nullptr) {
//        left_child_filter = left_child_->get_filter(block);
//    }
//    if (right_child_ != nullptr) {
//        right_child_filter = right_child_->get_filter(block);
//    }
//
//    auto memory_pool = arrow::default_memory_pool();
//    arrow::compute::FunctionContext function_context(memory_pool);
//    arrow::compute::CompareOptions compare_options(compare_operator_);
//    auto* block_filter = new arrow::compute::Datum();
//
//    // if the operator is a leaf, just perform the selction
//    if (left_child_ == nullptr && right_child_ == nullptr) {
//
//        auto select_col = block->get_column_by_name
//                (column_name_);
//        status = arrow::compute::Compare(&function_context,
//                                        select_col,
//                                        column_value_,
//                                        compare_options,
//                                        block_filter);
//        evaluate_status(status, __FUNCTION__, __LINE__);
//        }
//
//    else {
//        switch(filter_operator_) {
//            case AND: {
//                status = arrow::compute::And(&function_context, *left_child_filter,
//                                             *right_child_filter, block_filter);
//                evaluate_status(status, __FUNCTION__, __LINE__);
//            }
//            case OR: {
//                status = arrow::compute::Or(&function_context, *left_child_filter,
//                                            *right_child_filter, block_filter);
//                evaluate_status(status, __FUNCTION__, __LINE__);
//            }
//        }
//    }


std::shared_ptr<Table> Select::runOperator
(std::vector<std::shared_ptr<Table>> tables) {


    arrow::Status status;
    // operator only uses first table
    auto table = tables[0];

    std::shared_ptr<arrow::ChunkedArray> filter = get_filter(table);

    // Check if selectivity is 0 i.e. no tuples match the select predicate
    // TODO(nicholas): this is never true!
    if (filter->null_count() == filter->length() ) {
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;
    std::shared_ptr<arrow::ChunkedArray> out_col;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(compare_operator_);

    for (int i = 0; i < table->get_schema()->num_fields(); i++) {
        status = arrow::compute::Filter(&function_context,
                                        *table->get_column(i),
                                        *filter,
                                        &out_col);

        evaluate_status(status, __FUNCTION__, __LINE__);
        out_table_data.push_back(out_col);
    }

    // TODO(nicholas): Throw all of this into a new Table function.
    std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;
    auto out_table = std::make_shared<Table>("out", table->get_schema(),
                                             BLOCK_SIZE);

    for (int chunk_i=0; chunk_i<out_table_data[0]->num_chunks();
         chunk_i++) {
        for (auto &col : out_table_data) {
            out_block_data.push_back(col->chunk(chunk_i)->data());
        }

        out_table->insert_records(out_block_data);
        out_block_data.clear();
    }

    return out_table;
}

} // namespace operators
} // namespace hustle
