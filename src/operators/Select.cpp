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
    filters_.resize(table_->get_num_blocks());
}

arrow::Datum
Select::get_filter(const std::shared_ptr<Node> &node,
                   const std::shared_ptr<Block> &block,
                   bool is_and) {

    arrow::Status status;

    if (node->is_leaf()) {
        return get_filter(node->predicate_, block, is_and);
    }

    arrow::Datum block_filter;

    switch (node->connective_) {
        case AND: {
            auto left_child_filter = get_filter(node->left_child_, block, true);
            auto right_child_filter = get_filter(node->right_child_, block, true);
            return right_child_filter;
            break;
        }
        case OR: {
            auto left_child_filter = get_filter(node->left_child_, block, false);
            auto right_child_filter = get_filter(node->right_child_, block, false);
            status = arrow::compute::Or(left_child_filter, right_child_filter).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);
            return block_filter;
            break;
        }
        case NONE: {
            auto left_child_filter = get_filter(node->left_child_, block, false);
            return left_child_filter;
        }
    }
}

template<typename Functor>
void Select::for_each_batch(int batch_size, int num_batches, std::shared_ptr<arrow::ArrayVector> filter_vector, const Functor &functor) {
    for (int batch_i=0; batch_i<num_batches; batch_i++) {
        functor(filter_vector, batch_i, batch_size);
    }
}

void Select::execute(Task *ctx) {

    auto filter_vector = std::make_shared<arrow::ArrayVector>();
    filter_vector->resize(table_->get_num_blocks());

    ctx->spawnTask(CreateTaskChain(
        // Task 1: perform selection on all blocks
        CreateLambdaTask([this, filter_vector](Task *internal){

            int batch_size = table_->get_num_blocks() / 8;
//            batch_size = 1;
            if (batch_size == 0) batch_size = table_->get_num_blocks();
            int num_batches = table_->get_num_blocks() / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
            if (num_batches == 0) num_batches = 1;

            auto batch_functor = [&](auto filter_vector, auto batch_i, auto batch_size) {
                internal->spawnLambdaTask([this, filter_vector, batch_i, batch_size]() {
                    int base_i = batch_i * batch_size;
                    for (int i=base_i; i<base_i+batch_size && i<table_->get_num_blocks(); i++) {
                        execute_block(*filter_vector, i);
                    }
                });
            };

            for_each_batch(batch_size, num_batches, filter_vector, batch_functor);
        }),
        // Task 2: create the output result
        CreateLambdaTask([this, filter_vector](Task* internal) {
            finish(filter_vector, internal);
        })
    ));
}

void Select::execute_block(arrow::ArrayVector& filter_vector, int i) {
    auto block = table_->get_block(i);
    auto block_filter = this->get_filter(tree_->root_, block, false);
    filter_vector[i] = block_filter.make_array();
}

void Select::finish(std::shared_ptr<arrow::ArrayVector> filter_vector, Task* ctx) {

    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(*filter_vector);
    LazyTable lazy_table(table_, chunked_filter,arrow::Datum());
    output_result_->append(lazy_table);
}

arrow::Datum Select::get_filter(
    const std::shared_ptr<Predicate> &predicate,
    const std::shared_ptr<Block> &block,
    bool is_and) {

    arrow::Status status;

    arrow::compute::CompareOptions compare_options(predicate->comparator_);
    arrow::Datum block_filter;

    auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);
    auto value = predicate->value_;
//        std::cout << predicate->col_ref_.col_name << std::endl;

    if (is_and){
        if (filters_[block->get_id()] == nullptr) {

            status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);

            filters_[block->get_id()] = block_filter.make_array();
        } else {
            switch(select_col->type_id()){
                case arrow::Type::INT64: {
                    auto data = arrow::ArrayData::Make(select_col->type(), select_col->length(),
                                                       {filters_[block->get_id()]->data()->buffers[1],
                                                        select_col->data()->buffers[1]});
                    select_col = arrow::MakeArray(data);
                    break;
                }
                case arrow::Type::STRING: {
                    auto data = arrow::ArrayData::Make(select_col->type(), select_col->length(),
                                                       {filters_[block->get_id()]->data()->buffers[1],
                                                        select_col->data()->buffers[1],
                                                        select_col->data()->buffers[2]});
                    select_col = arrow::MakeArray(data);
                }
            }

            status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);

            auto temp = block_filter.array();

            auto is_true = std::make_shared<arrow::BooleanArray>(temp->length, temp->buffers[1]);
            auto is_valid = std::make_shared<arrow::BooleanArray>(temp->length, temp->buffers[0]);
            status = arrow::compute::And(is_true, is_valid).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);

            filters_[block->get_id()] = block_filter.make_array();
        }
    } else {
        status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }
    
    return block_filter;

}

} // namespace operators
} // namespace hustle