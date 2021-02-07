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
#define TK_AGG_COLUMN 166
#define TK_PLUS 105
#define TK_MINUS 106
#define TK_STAR 107
#define TK_SLASH 108

namespace hustle {
namespace operators {

struct ExprElem {
  int op;
  std::shared_ptr<arrow::ChunkedArray> col;
};

struct OpElem {
  int op;
  bool is_result;
  arrow::Datum chunk;
};

typedef struct ExprElem ExprElem;
typedef struct OpElem OpElem;

class Expression {
 private:
  std::shared_ptr<ExprReference> expr_;
  std::vector<ExprElem> postfix_expr_;
  OperatorResult::OpResultPtr prev_op_output_;

  int32_t exp_num_chunks_;

  void ConvertPostfix(hustle::Task* ctx, std::shared_ptr<ExprReference> expr);

 public:
  Expression(OperatorResult::OpResultPtr prev_op_output,
             std::shared_ptr<ExprReference> expr);

  void Initialize(hustle::Task* ctx);

  template <typename ArrayType, typename ArrayPrimitiveType>
  arrow::Datum ExecuteBlock(int op, std::shared_ptr<arrow::Array> result,
                            std::shared_ptr<arrow::Array> left_col,
                            std::shared_ptr<arrow::Array> right_col);

  template <typename ArrayType, typename ArrayPrimitiveType>
  arrow::Datum ExecuteBlock(bool is_result, const arrow::Scalar& scalar, int op,
                            std::shared_ptr<arrow::Array> left_col,
                            std::shared_ptr<arrow::Array> right_col);

  arrow::Datum Evaluate(hustle::Task* ctx, int chunk_id);

  inline int32_t num_chunks() { return exp_num_chunks_; }
};
}  // namespace operators
}  // namespace hustle

#endif