#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {

Select::Select(
    const std::size_t query_id,
    std::shared_ptr<OperatorResult> prev_result,
    std::shared_ptr<OperatorResult> output_result,
    std::shared_ptr<PredicateTree> tree) : Operator(query_id) {

    prev_result_ = prev_result;
    output_result_ = output_result;
    tree_ = tree;

    auto node = tree_->root_;

    // We can figure out which table we are performing the selection on if we
    // look at the LHS of one of the leaf nodes.
    while (!node->is_leaf()) {
        node = node->left_child_;
    }
    table_ = node->predicate_->col_ref_.table;

    auto select_col = table_->get_column_by_name(node->predicate_->col_ref_.col_name);

    left_vector_.reserve(select_col->num_chunks());
    right_vector_.resize(select_col->num_chunks());
    for (auto& chunk : select_col->chunks()) {
        left_vector_.push_back(chunk);
    }
}

arrow::Datum
Select::get_filter(const std::shared_ptr<Node> &node,
                   const std::shared_ptr<Block> &block,
                   const std::shared_ptr<arrow::Array>& prev_filter) {

    arrow::Status status;

    if (node->is_leaf()) {
        return get_filter(node->predicate_, block, prev_filter);
    }


    arrow::Datum block_filter;

    switch (node->connective_) {
        case AND: {
            auto left_child_filter = get_filter(node->left_child_, block, nullptr);
            auto right_child_filter = get_filter(node->right_child_, block, nullptr);

//            auto right_child_filter = get_filter(node->right_child_, block, left_child_filter.make_array());
//            block_filter = right_child_filter;
            status = arrow::compute::And(left_child_filter, right_child_filter).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case OR: {
            auto left_child_filter = get_filter(node->left_child_, block, nullptr);
            auto right_child_filter = get_filter(node->right_child_, block, nullptr);
            status = arrow::compute::Or(left_child_filter, right_child_filter).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case NONE: {
            auto left_child_filter = get_filter(node->left_child_, block, nullptr);
            block_filter = left_child_filter;
        }
    }
    return block_filter;
}

void Select::execute(Task *ctx) {

    ctx->spawnTask(CreateTaskChain(
        // Task 1: perform selection on all blocks
        CreateLambdaTask([this](Task *internal){
            filter_vector_.resize(table_->get_num_blocks());

            int batch_size = table_->get_num_blocks() / 8;
            if (batch_size == 0) batch_size = table_->get_num_blocks();
            int num_batches = table_->get_num_blocks() / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
            if (num_batches == 0) num_batches = 1;

            for (int batch_i=0; batch_i<num_batches; batch_i++) {
                // Each task gets the filter for one block and stores it in filter_vector
                internal->spawnLambdaTask([this, batch_i, batch_size]() {
                    int base_i = batch_i * batch_size;
                    for (int i=base_i; i<base_i+batch_size && i<table_->get_num_blocks(); i++) {
                        execute_block(i);
                    }
                });
            }
        }),
        // Task 2: create the output result
        CreateLambdaTask([this]() {
            finish();
        })
    ));
}

void Select::execute_block(int i) {
    auto block = table_->get_block(i);
    auto block_filter = this->get_filter(tree_->root_, block, nullptr);
    filter_vector_[i] = block_filter.make_array();
}

void Select::finish() {

    arrow::Status status;

    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(filter_vector_);
    auto length = chunked_filter->length();

    arrow::UInt64Builder indices_builder;
    status = indices_builder.Resize(length);
    evaluate_status(status, __FUNCTION__, __LINE__);

    int offset = 0;
    for (int i=0; i<right_vector_.size(); i++) {

        offset = table_->get_block_row_offset(i);
        arrow::Datum temp;
        status = arrow::compute::internal::GetTakeIndices(*chunked_filter->chunk(i)->data(), arrow::compute::FilterOptions::EMIT_NULL).Value(&temp);
        evaluate_status(status, __FUNCTION__, __LINE__);
        auto chunk_indices = temp.make_array();
        auto indices_length = chunk_indices->length();

        // TODO(nicholas): data will be of type uint32_t if block size is large!
        auto data =chunk_indices->data()->GetMutableValues<uint16_t>(1);

        for (int j=0; j<indices_length; j++) {
            indices_builder.UnsafeAppend(data[j] + offset);
//            std::cout << "BEFORE " << data[j] << std::endl;
//            std::cout << "AFTER " << data[j] << " " << offset2 << std::endl;
        }
    }

    std::shared_ptr<arrow::Array> indices;
    status = indices_builder.Finish(&indices);
    evaluate_status(status, __FUNCTION__, __LINE__);

    LazyTable lazy_table(table_, arrow::Datum(), indices);
    output_result_->append(lazy_table);
}

arrow::Datum Select::get_filter(
    const std::shared_ptr<Predicate> &predicate,
    const std::shared_ptr<Block> &block,
    const std::shared_ptr<arrow::Array>& prev_filter) {

    arrow::Status status;

    arrow::compute::CompareOptions compare_options(predicate->comparator_);
    arrow::Datum block_filter;

    auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);

    if (prev_filter != nullptr) {
        arrow::Datum temp;
        status = arrow::compute::Filter(select_col, prev_filter).Value(&temp);
        select_col = temp.make_array();
    }

    auto value = predicate->value_;

    status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
    evaluate_status(status, __FUNCTION__, __LINE__);

    return block_filter;

}

arrow::Datum Select::get_filter(
    const std::shared_ptr<Predicate> &predicate,
    const std::shared_ptr<Block> &block) {

    arrow::Status status;
    
    arrow::compute::CompareOptions compare_options(predicate->comparator_);
    arrow::Datum block_filter;

    auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);
    auto value = predicate->value_;

    status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
    evaluate_status(status, __FUNCTION__, __LINE__);

    return block_filter;

}

} // namespace operators
} // namespace hustle