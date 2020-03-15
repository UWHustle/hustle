#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>

#define BLOCK_SIZE 1024

namespace hustle {
namespace operators {

SelectComposite::SelectComposite(
        std::shared_ptr<Select> left_child,
        std::shared_ptr<Select> right_child,
        FilterOperator filter_operator) {
  left_child_ = left_child;
  right_child_ = right_child;
  filter_operator_ = filter_operator;
}

arrow::compute::Datum* SelectComposite::get_filter(std::shared_ptr<Block>
        block) {

    arrow::Status status;

    auto* left_child_filter = left_child_->get_filter(block);
    auto* right_child_filter = right_child_->get_filter(block);

    auto testleft = left_child_filter->array();
    auto testright = right_child_filter->array();
    // We should never have to check for null children, since
    // SelectComposite is only relevant if it has two children. Otherwise,
    // we would just use Select.

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    auto* block_filter = new arrow::compute::Datum();

    switch(filter_operator_) {
        case AND: {
            status = arrow::compute::And(&function_context, *left_child_filter,
                                         *right_child_filter, block_filter);
            auto test = block_filter->array();
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case OR: {
            status = arrow::compute::Or(&function_context, *left_child_filter,
                                        *right_child_filter, block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
    }

    return block_filter;

}

    std::shared_ptr<Table> SelectComposite::runOperator
            (std::vector<std::shared_ptr<Table>> tables) {



        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];
        auto out_table = std::make_shared<Table>("out", table->get_schema(),
                                                 BLOCK_SIZE);

        std::vector<std::shared_ptr<arrow::ArrayData>> out_cols;
        auto* out_col = new arrow::compute::Datum;
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

        for (int i=0; i<table->get_num_blocks(); i++) {
            auto block = table->get_block(i);
            auto block_filter = get_filter(block);

            for (int i = 0; i < table->get_schema()->num_fields(); i++) {
                status = arrow::compute::Filter(&function_context,
                                                block->get_column(i),
                                                *block_filter,
                                                out_col);

                evaluate_status(status, __FUNCTION__, __LINE__);
                out_cols.push_back(out_col->array());
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


    arrow::compute::Datum* Select::get_filter
            (std::shared_ptr<Block> block) {

        arrow::Status status;

        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::CompareOptions compare_options(compare_operator_);
        auto* block_filter = new arrow::compute::Datum();

        auto select_col = block->get_column_by_name(column_name_);

        auto j = column_value_.type()->name();

        status = arrow::compute::Compare(&function_context,
                                         select_col,
                                         column_value_,
                                         compare_options,
                                         block_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        return block_filter;
    }


    std::shared_ptr<Table> Select::runOperator
            (std::vector<std::shared_ptr<Table>> tables) {


        arrow::Status status;
        // operator only uses first table
        auto table = tables[0];
        auto out_table = std::make_shared<Table>("out", table->get_schema(),
                                                 BLOCK_SIZE);

        std::vector<std::shared_ptr<arrow::ArrayData>> out_cols;
        auto* out_col = new arrow::compute::Datum;
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

        for (int i=0; i<table->get_num_blocks(); i++) {
            auto block = table->get_block(i);
            auto block_filter = get_filter(block);

            for (int i = 0; i < table->get_schema()->num_fields(); i++) {
                status = arrow::compute::Filter(&function_context,
                                                block->get_column(i),
                                                *block_filter,
                                                out_col);

                evaluate_status(status, __FUNCTION__, __LINE__);
                out_cols.push_back(out_col->array());
            }
            out_table->insert_records(out_cols);
            out_cols.clear();
        }

        return out_table;
    }

} // namespace operators
} // namespace hustle