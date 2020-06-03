#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <table/Index.h>
#include <iostream>
#include <map>


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
    left_filter_vector_.resize(table_->get_num_blocks());
    right_filter_vector_.resize(table_->get_num_blocks());

}

void Select::get_predicate_filter(
    Task* ctx,
    std::shared_ptr<Node> node,
    arrow::ArrayVector& out){

    for (int i = 0; i < table_->get_num_blocks(); i++) {
        // Each task gets the filter for one block and stores it in filter_vector
        ctx->spawnLambdaTask([this, &out, node, i]() {
            auto block = table_->get_block(i);
            auto block_filter = this->get_block_filter(node->predicate_, block);
            out[i] = block_filter.make_array();
        });
    }
}

void Select::combine_filters(std::shared_ptr<Node> node) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::Datum block_filter;

    // TODO(nicholas): spawn new task for each Array
    switch (node->connective_) {
        case AND: {
            for (int i=0; i<table_->get_num_blocks(); i++) {
                status = arrow::compute::And(
                    &function_context, left_filter_vector_[i], right_filter_vector_[i],
                    &block_filter);
                evaluate_status(status, __FUNCTION__, __LINE__);
                left_filter_vector_[i] = block_filter.make_array();
            }

            break;
        }
        case OR: {
            for (int i=0; i<table_->get_num_blocks(); i++) {
                status = arrow::compute::Or(
                    &function_context, left_filter_vector_[i], right_filter_vector_[i],
                    &block_filter);
                evaluate_status(status, __FUNCTION__, __LINE__);
                left_filter_vector_[i] = block_filter.make_array();
            }
            break;
        }
        case NONE: {
            break;
        }
    }
}

void Select::traverse_predicate_tree(std::vector<Task*>& tasks, std::shared_ptr<Node> node, int side) {

    if (node->is_leaf()) {
        if (side == 0) {
            tasks.push_back(CreateLambdaTask([this, node](Task* internal) {
                get_predicate_filter(internal, node, left_filter_vector_);
            }));
        } else {
            tasks.push_back(CreateLambdaTask([this, node](Task* internal) {
                get_predicate_filter(internal, node, right_filter_vector_);
            }));
        }
        return;
    }

    tasks.push_back(CreateLambdaTask([this, node] {
        combine_filters(node);
    }));

    traverse_predicate_tree(tasks, node->left_child_, 0);
    traverse_predicate_tree(tasks, node->right_child_, 1);

}

std::vector<Task*> Select::traverse_predicate_tree() {

    std::vector<Task*> tasks;
    traverse_predicate_tree(tasks, tree_->root_, 0);
    return tasks;

}

void Select::execute(Task *ctx) {

    auto tasks = traverse_predicate_tree();
    std::reverse(tasks.begin(), tasks.end());
    tasks.push_back(CreateLambdaTask([this]() {
        finish();
    }));

    ctx->spawnTask(CreateTaskChain(tasks));

}

void Select::finish() {
    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(left_filter_vector_);
    arrow::compute::Datum filter(chunked_filter);

    LazyTable result_unit(table_, filter, arrow::compute::Datum());
    OperatorResult result({result_unit});
    output_result_->append(std::make_shared<OperatorResult>(result));
}

arrow::compute::Datum Select::get_block_filter(
    const std::shared_ptr<Predicate> &predicate,
    const std::shared_ptr<Block> &block) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(predicate->comparator_);
    arrow::compute::Datum block_filter;

    auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);
    auto value = predicate->value_;

    status = arrow::compute::Compare(
        &function_context, select_col, value, compare_options, &block_filter);
    evaluate_status(status, __FUNCTION__, __LINE__);

    return block_filter;
}

} // namespace operators
} // namespace hustle