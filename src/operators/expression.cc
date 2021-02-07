// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "operators/expression.h"

#include <algorithm>
#include <cassert>

#include "scheduler/threading/synchronization_lock.h"

namespace hustle::operators {

Expression::Expression(OperatorResult::OpResultPtr prev_op_output,
                       std::shared_ptr<ExprReference> expr)
    : expr_(expr), prev_op_output_(prev_op_output) {
  exp_num_chunks_ = 0;
}

void Expression::Initialize(hustle::Task* ctx) {
  if (expr_ == nullptr) {
    throw std::runtime_error("Expression is expected here.");
  }
  this->ConvertPostfix(ctx, expr_);
}

void Expression::ConvertPostfix(hustle::Task* ctx,
                                std::shared_ptr<ExprReference> expr) {
  if (expr == nullptr) {
    return;
  }
  this->ConvertPostfix(ctx, expr->left_expr);
  this->ConvertPostfix(ctx, expr->right_expr);

  std::shared_ptr<arrow::ChunkedArray> column = nullptr;
  if (expr->op == TK_COLUMN || expr->op == TK_AGG_COLUMN) {
    arrow::Datum col;
    prev_op_output_->get_table(expr->column_ref->table)
        .get_column_by_name(ctx, expr->column_ref->col_name, col);
    column = col.chunked_array();
    exp_num_chunks_ = std::max(exp_num_chunks_, column->num_chunks());
  }
  postfix_expr_.push_back({expr->op, column});
}

template <typename ArrayType, typename ArrayPrimitiveType>
arrow::Datum Expression::ExecuteBlock(int op,
                                      std::shared_ptr<arrow::Array> result,
                                      std::shared_ptr<arrow::Array> left_col,
                                      std::shared_ptr<arrow::Array> right_col) {
  /*std::shared_ptr<ArrayType> result,
  std::shared_ptr<ArrayType> left_col,
  std::shared_ptr<ArrayType> right_col) {*/
  int chunk_length = std::max(left_col->length(), right_col->length());
  auto left_col_data =
      left_col->data()->template GetValues<ArrayPrimitiveType>(1, 0);
  auto right_col_data =
      right_col->data()->template GetValues<ArrayPrimitiveType>(1, 0);
  std::shared_ptr<ArrayType> result_arr =
      std::static_pointer_cast<ArrayType>(result);

  auto result_data =
      result->data()->template GetMutableValues<ArrayPrimitiveType>(1, 0);
  switch (op) {
    case TK_PLUS: {
      for (std::size_t row = 0; row < chunk_length; row++) {
        result_data[row] = left_col_data[row] + right_col_data[row];
      }
      break;
    }
    case TK_MINUS: {
      for (std::size_t row = 0; row < chunk_length; row++) {
        result_data[row] = left_col_data[row] - right_col_data[row];
      }
      break;
    }
    case TK_SLASH: {
      for (std::size_t row = 0; row < chunk_length; row++) {
        result_data[row] = left_col_data[row] / right_col_data[row];
      }
      break;
    }
    case TK_STAR: {
      for (std::size_t row = 0; row < chunk_length; row++) {
        result_data[row] = left_col_data[row] * right_col_data[row];
      }
      break;
    }
  }
  return result;
}

template <typename ArrayType, typename ArrayPrimitiveType>
arrow::Datum Expression::ExecuteBlock(bool is_result,
                                      const arrow::Scalar& scalar, int op,
                                      std::shared_ptr<arrow::Array> left_col,
                                      std::shared_ptr<arrow::Array> right_col) {
  // std::shared_ptr<ArrayType> left_col,
  // std::shared_ptr<ArrayType> right_col) {
  if (is_result) {
    std::shared_ptr<ArrayType> result = std::static_pointer_cast<ArrayType>(
        arrow::MakeArrayFromScalar(scalar, left_col->length()).ValueOrDie());
    return this->ExecuteBlock<ArrayType, ArrayPrimitiveType>(
        op, result, left_col, right_col);
  } else {
    return this->ExecuteBlock<ArrayType, ArrayPrimitiveType>(
        op, left_col, left_col, right_col);
  }
}

arrow::Datum Expression::Evaluate(hustle::Task* ctx, int chunk_id) {
  std::stack<OpElem> eval_stack;
  arrow::Datum result;

  assert((postfix_expr_.size() != 0) && "Initialize() the expression object");
  for (int i = 0; i < postfix_expr_.size(); i++) {
    ExprElem expr_item = postfix_expr_[i];
    if (expr_item.op == TK_COLUMN || expr_item.op == TK_AGG_COLUMN) {
      eval_stack.push({expr_item.op, false, expr_item.col->chunk(chunk_id)});
    } else {
      OpElem left_op = eval_stack.top();
      eval_stack.pop();
      OpElem right_op = eval_stack.top();
      eval_stack.pop();

      bool is_result = false;
      if (!right_op.is_result && !right_op.is_result) {
        is_result = true;
      }
      int op = expr_item.op;
      auto l_chunk = left_op.chunk.make_array();
      auto r_chunk = right_op.chunk.make_array();

      if (l_chunk->type() != r_chunk->type()) {
        throw std::runtime_error(
            "Implicit type conversion in"
            "expression evaluation currently not supported.");
      }
      switch (l_chunk->type()->id()) {
        case arrow::Type::INT64: {
          result =
              this->ExecuteBlock<arrow::Int64Array, int64_t>(
                      is_result, arrow::Int64Scalar(0), op, l_chunk, r_chunk)
                  .make_array();
          break;
        }
        default: {
          throw std::runtime_error(
              "Expression Evaluation"
              "not supported for this type.");
        }
      }
      eval_stack.push({TK_AGG_COLUMN, true, arrow::Datum(result)});
    }
  }
  if (!eval_stack.empty()) {
    return arrow::Datum(eval_stack.top().chunk);
  }
  throw std::runtime_error("Invalid expression");
}

}  // namespace hustle::operators
