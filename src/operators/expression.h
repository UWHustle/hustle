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
#include "type/type_helper.h"

#define TK_COLUMN 164
#define TK_AGG_COLUMN 166
#define TK_PLUS 105
#define TK_MINUS 106
#define TK_STAR 107
#define TK_SLASH 108

namespace hustle {
namespace operators {

// Elements in the postfix expression
struct ExprElem {
  int op;
  std::shared_ptr<arrow::ChunkedArray> col;
};

// Elements for evaluation in Stack
struct OpElem {
  int op;
  bool is_result;
  arrow::Datum chunk;
};

typedef struct ExprElem ExprElem;
typedef struct OpElem OpElem;

/**
 * This class helps in evaluating the expression in the SQL query
 * and return the result to the operator at which it is called.
 *
 * It converts the expression to postfix and reuse the single result output
 * for all the operator evaluation.
 *
 * It operates on a chunk level, hence it can be integrated to the multithreaded
 * code which operate on a chunk. However, creation and initialization of the
 * object needed to be done only once.
 *
 * Currently, expression columns all belong single table only supported.
 * */
class Expression {
 private:
  // Input expression
  std::shared_ptr<ExprReference> expr_;
  // Postfix format of the input expression
  std::vector<ExprElem> postfix_expr_;
  // Output result of the previous operator
  OperatorResult::OpResultPtr prev_op_output_;

  int32_t exp_num_chunks_;

  void ConvertPostfix(hustle::Task* ctx, std::shared_ptr<ExprReference> expr);

  // Execute an arithmetic operator for two column of the chunk in the table
  template <typename ArrayType, typename ArrayPrimitiveType>
  arrow::Datum ExecuteBlock(int op, std::shared_ptr<arrow::Array> result,
                            const std::shared_ptr<arrow::Array>& left_col,
                            const std::shared_ptr<arrow::Array>& right_col);

  template <typename ArrayType, typename ArrayPrimitiveType>
  arrow::Datum ExecuteBlockAPI(bool is_result, const arrow::Scalar& scalar, int op,
                            std::shared_ptr<arrow::Array> left_col,
                            std::shared_ptr<arrow::Array> right_col);

 public:
  Expression(OperatorResult::OpResultPtr prev_op_output,
             std::shared_ptr<ExprReference> expr);

  // Initializes the expression and its corresponding postfix in the object
  void Initialize(hustle::Task* ctx);

  // Starting point to invoke the evaluation of the expression for a chunk
  arrow::Datum Evaluate(hustle::Task* ctx, int chunk_id);

  // number of chunks present in the input column on which it operates.
  [[nodiscard]] inline int32_t num_chunks() const { return exp_num_chunks_; }
};
}  // namespace operators
}  // namespace hustle

#endif
