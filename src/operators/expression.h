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

#ifndef HUSTLE_EXPRESSION_H
#define HUSTLE_EXPRESSION_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <memory>
#include <stack>
#include <string>
#include <vector>

#include "operators/operator.h"
#include "operators/utils/operator_result.h"

#define TK_COLUMN 164
#define TK_PLUS 105
#define TK_MINUS 106
#define TK_STAR 107
#define TK_SLASH 108

namespace hustle {
namespace operators {

struct ExprElem {
  int op;
  bool is_result_col;
  std::shared_ptr<arrow::ChunkedArray> col;
};

class Expression {
 private:
  std::shared_ptr<ExprReference> expr_;
  std::vector<std::shared_ptr<arrow::Array>> result_vector_;

  void ConvertPostFix(std::shared_ptr<ExprReference> expr,
                      std::vector<ExprElem>& postfix_expr) {
    if (expr == nullptr) {
      return;
    }
    this->ConvertPostFix(expr->left_expr, postfix_expr);
    this->ConvertPostFix(expr->right_expr, postfix_expr);

    std::shared_ptr<arrow::ChunkedArray> column = nullptr;
    if (expr->op == TK_COLUMN) {
      column = expr->column_ref->table->get_column_by_name(
          expr->column_ref->col_name);
    }
    postfix_expr.push_back({expr->op, false, column});
  }

 public:
  Expression(std::shared_ptr<ExprReference> expr) : expr_(expr) {}

  arrow::Datum ExecuteBlock(int op, std::shared_ptr<arrow::Array> result,
                            std::shared_ptr<arrow::Array> col_1,
                            std::shared_ptr<arrow::Array> col_2) {
    int chunk_length = col_1->length();
    auto col_data_1 = col_1->data()->GetValues<uint64_t>(1, 0);
    auto col_data_2 = col_2->data()->GetValues<uint64_t>(1, 0);
    auto result_arr = std::static_pointer_cast<arrow::Int64Array>(result);

    auto result_data = result_arr->values()->mutable_data();
    switch (op) {
      case TK_PLUS: {
        for (std::size_t row = 0; row < chunk_length; row++) {
          result_data[row] = col_data_1[row] + col_data_2[row];
        }
        break;
      }
      case TK_MINUS: {
        for (std::size_t row = 0; row < chunk_length; row++) {
          result_data[row] = col_data_1[row] - col_data_2[row];
        }
        break;
      }
      case TK_SLASH: {
        for (std::size_t row = 0; row < chunk_length; row++) {
          result_data[row] = col_data_1[row] / col_data_2[row];
        }
        break;
      }
      case TK_STAR: {
        for (std::size_t row = 0; row < chunk_length; row++) {
          result_data[row] = col_data_1[row] * col_data_2[row];
        }
        break;
      }
    }
    return result;
  }

  void EvaluateExpression(Task* ctx, arrow::Datum& out) {
    if (expr_ == nullptr) {
      return;
    }
    std::vector<ExprElem> postfix_expr(10);
    // result_vector_.resize(col_1->num_chunks());

    this->ConvertPostFix(expr_, postfix_expr);

    // result_vector.push_back(nullptr);

    std::stack<ExprElem> eval_stack;
    for (ExprElem expr_item : postfix_expr) {
      if (expr_item.op == TK_COLUMN) {
        if (result_vector_.size() < expr_item.col->num_chunks()) {
          result_vector_.resize(expr_item.col->num_chunks());
        }
        eval_stack.push(expr_item);
      } else {
        ExprElem expr_item1 = eval_stack.top();
        eval_stack.pop();
        ExprElem expr_item2 = eval_stack.top();
        eval_stack.pop();
        bool is_result = false;

        auto col_1 = expr_item1.col;
        auto col_2 = expr_item2.col;

        if (!expr_item1.is_result_col && !expr_item2.is_result_col) {
          is_result = true;
        }

        int batch_size =
            col_1->num_chunks() / std::thread::hardware_concurrency() / 2;
        if (batch_size == 0) batch_size = col_1->num_chunks();
        int num_batches = (col_1->num_chunks() / batch_size) + 1;
        int op = expr_item.op;
        for (std::size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
          ctx->spawnLambdaTask(
              [this, batch_idx, batch_size, op, col_1, col_2, is_result] {
                int base_idx = batch_idx * batch_size;
                int num_chunks = col_1->num_chunks();
                int chunk_limit = base_idx + batch_size;
                for (size_t chunk_idx = base_idx;
                     (chunk_idx < chunk_limit && chunk_idx < num_chunks);
                     ++chunk_idx) {
                  auto chunk_1 = col_1->chunk(chunk_idx);
                  auto chunk_2 = col_2->chunk(chunk_idx);
                  if (is_result) {
                    std::shared_ptr<arrow::Int64Array> result =
                        std::static_pointer_cast<arrow::Int64Array>(
                            arrow::MakeArrayFromScalar(arrow::Int64Scalar(0),
                                                       num_chunks)
                                .ValueOrDie());
                    result_vector_[chunk_idx] =
                        this->ExecuteBlock(op, result, chunk_1, chunk_2)
                            .make_array();
                  } else {
                    result_vector_[chunk_idx] =
                        this->ExecuteBlock(op, chunk_1, chunk_1, chunk_2)
                            .make_array();
                  }
                }
              });
        }
        ExprElem result_expr_item = {
            TK_COLUMN, false,
            std::make_shared<arrow::ChunkedArray>(result_vector_)};
        eval_stack.push(result_expr_item);
      }
    }
    if (!eval_stack.empty()) {
      ExprElem result_expr_item = eval_stack.top();
      out = std::make_shared<arrow::ChunkedArray>(result_vector_);
    }
  }
};
}  // namespace operators
}  // namespace hustle

#endif