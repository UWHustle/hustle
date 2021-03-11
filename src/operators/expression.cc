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
#include "type/type_helper.h"

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
  // Push the expression elements along with needed metadata for the postfix
  // output
  if (expr->op == TK_COLUMN || expr->op == TK_AGG_COLUMN) {
    arrow::Datum col;
    prev_op_output_->get_table(expr->column_ref->table)
        .MaterializeColumn(ctx, expr->column_ref->col_name, col);
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
  int chunk_length = std::max(left_col->length(), right_col->length());
  auto left_col_data =
      left_col->data()->template GetValues<ArrayPrimitiveType>(1, 0);
  auto right_col_data =
      right_col->data()->template GetValues<ArrayPrimitiveType>(1, 0);
  std::shared_ptr<ArrayType> result_arr =
      std::static_pointer_cast<ArrayType>(result);

  // mutable result data
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

template <typename T>
std::enable_if_t<arrow::has_c_type<T>::value, arrow::Datum>
Expression::ExecuteBlockHandler(
    bool is_result, int op, const std::shared_ptr<arrow::Array>& left_col,
    const std::shared_ptr<arrow::Array>& right_col) {
  using ArrayType = typename arrow::TypeTraits<T>::ArrayType;
  using ScalarType = typename arrow::TypeTraits<T>::ScalarType;
  using CType = typename T::c_type;
  return this->ExecuteBlock<ArrayType, CType>(is_result, ScalarType(0), op,
                                              left_col, right_col);
};

template <typename T>
std::enable_if_t<!arrow::has_c_type<T>::value, arrow::Datum>
Expression::ExecuteBlockHandler(
    bool is_result, int op, const std::shared_ptr<arrow::Array>& left_col,
    const std::shared_ptr<arrow::Array>& right_col) {
  throw std::runtime_error(
      "Expression Evaluation"
      "not supported for this type.");
}

// Execute block handler does not support date time interval.
template <>
arrow::Datum Expression::ExecuteBlockHandler<arrow::DayTimeIntervalType>(
    bool is_result, int op, const std::shared_ptr<arrow::Array>& left_col,
    const std::shared_ptr<arrow::Array>& right_col) {
  throw std::runtime_error(
      "Expression Evaluation"
      "not supported for this DateTimeIntervalType.");
};

arrow::Datum Expression::Evaluate(hustle::Task* ctx, int chunk_id) {
  // stack for the expression evaluation on the chunk
  std::stack<OpElem> eval_stack;
  // To store the result and also reused to store the temporary result.
  arrow::Datum result;

  assert((postfix_expr_.size() != 0) && "Initialize() the expression object");
  for (int i = 0; i < postfix_expr_.size(); i++) {
    // expression element in postfix expression
    ExprElem expr_item = postfix_expr_[i];
    if (expr_item.op == TK_COLUMN || expr_item.op == TK_AGG_COLUMN) {
      // Push the column references into the stack which are operands
      eval_stack.push({expr_item.op, false, expr_item.col->chunk(chunk_id)});
    } else {  // if it is a arithmetic operator do the evaluation of the
              // operator
      OpElem right_op = eval_stack.top();
      eval_stack.pop();
      OpElem left_op = eval_stack.top();
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
      auto enum_type = l_chunk->type()->id();


#undef HUSTLE_ARROW_TYPE_CASE_STMT
#define HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_)                      \
  {                                                                        \
    result = this->ExecuteBlockHandler<arrow_data_type_>(is_result, op,    \
                                                         l_chunk, r_chunk) \
                 .make_array();                                            \
    break;                                                                 \
  }

      HUSTLE_SWITCH_ARROW_TYPE(enum_type);
#undef HUSTLE_ARROW_TYPE_CASE_STMT

      eval_stack.push({TK_AGG_COLUMN, true, arrow::Datum(result)});
    }
  }
  if (!eval_stack.empty()) {
    return arrow::Datum(eval_stack.top().chunk);
  }
  throw std::runtime_error("Invalid expression");
}  // namespace hustle::operators

}  // namespace hustle::operators
