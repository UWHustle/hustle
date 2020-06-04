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
}

arrow::compute::Datum
Select::get_filter(const std::shared_ptr<Node> &node,
                   const std::shared_ptr<Block> &block) {

  arrow::Status status;

  if (node->is_leaf()) {
    return get_filter(node->predicate_, block);
  }
  auto left_child_filter = get_filter(node->left_child_, block);
  auto right_child_filter = get_filter(node->right_child_, block);

  arrow::compute::FunctionContext function_context(
      arrow::default_memory_pool());
  arrow::compute::Datum block_filter;

  switch (node->connective_) {
    case AND: {
      status = arrow::compute::And(
          &function_context, left_child_filter, right_child_filter,
          &block_filter);
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }
    case OR: {
      status = arrow::compute::Or(
          &function_context, left_child_filter, right_child_filter,
          &block_filter);
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }
    case NONE: {
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

        for (int i = 0; i < table_->get_num_blocks(); i++) {

          // Each task gets the filter for one block and stores it in filter_vector
          internal->spawnLambdaTask([this, i]() {
            auto block = table_->get_block(i);
            auto block_filter = this->get_filter(tree_->root_, block);
            filter_vector_[i] = block_filter.make_array();
          });
        }
      }),
      // Task 2: create the output result
      CreateLambdaTask([this]() {
        finish();
      })
  ));

}

void Select::finish() {

  auto chunked_filter = std::make_shared<arrow::ChunkedArray>(filter_vector_);
  arrow::compute::Datum filter(chunked_filter);
  LazyTable lazy_table(table_, filter, arrow::compute::Datum());
  output_result_->append(lazy_table);
}

arrow::compute::Datum Select::get_filter(
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