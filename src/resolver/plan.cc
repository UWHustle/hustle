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

#include "plan.h"

namespace hustle {
namespace resolver {

void to_json(json &j, const std::shared_ptr<Plan> &plan) {
  switch (plan->type) {
    case PlanType::Query:
      j = json(std::dynamic_pointer_cast<Query>(plan));
      break;
    case PlanType::Create:
      j = json(std::dynamic_pointer_cast<Create>(plan));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<Query> &query) {
  j = json{{"type", query->type._to_string()},
           {"query_operator", query->query_operator}};
}
void to_json(json &j, const std::shared_ptr<Create> &create) {
  /// TODO(Lichengxi): serialization
}

void to_json(json &j, const std::shared_ptr<QueryOperator> &query_operator) {
  switch (query_operator->type) {
    case QueryOperatorType::TableReference:
      j = json(std::dynamic_pointer_cast<TableReference>(query_operator));
      break;
    case QueryOperatorType::Select:
      j = json(std::dynamic_pointer_cast<Select>(query_operator));
      break;
    case QueryOperatorType::Project:
      j = json(std::dynamic_pointer_cast<Project>(query_operator));
      break;
    case QueryOperatorType::Join:
      j = json(std::dynamic_pointer_cast<Join>(query_operator));
      break;
    case QueryOperatorType::Aggregate:
      j = json(std::dynamic_pointer_cast<Aggregate>(query_operator));
      break;
    case QueryOperatorType::OrderBy:
      j = json(std::dynamic_pointer_cast<OrderBy>(query_operator));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<TableReference> &table_reference) {
  j = json{
      {"type", table_reference->type._to_string()},
      {"i_table", table_reference->i_table},
      {"table_name", table_reference->table_name},
  };
}
void to_json(json &j, const std::shared_ptr<Select> &select) {
  j = json{
      {"type", select->type._to_string()},
      {"input", select->input},
      {"filter", select->filter},
  };
}
void to_json(json &j, const std::shared_ptr<Project> &project) {
  j = json{
      {"type", project->type._to_string()},
      {"input", project->input},
      {"proj_exprs", project->proj_exprs},
      {"proj_names", project->proj_names},
  };
}
void to_json(json &j, const std::shared_ptr<Join> &join) {
  j = json{
      {"type", join->type._to_string()},
      {"left_input", join->left_input},
      {"right_input", join->right_input},
      {"join_pred", join->join_pred},
  };
}
void to_json(json &j, const std::shared_ptr<Aggregate> &aggregate) {
  j = json{
      {"type", aggregate->type._to_string()},
      {"input", aggregate->input},
      {"aggregate_func", aggregate->aggregate_func},
      {"groupby_cols", aggregate->groupby_cols},
  };
}
void to_json(json &j, const std::shared_ptr<OrderBy> &orderby) {
  int size = orderby->orders.size();
  std::vector<std::string> orders(size);
  for (int i = 0; i < size; i++) {
    orders[i] = orderby->orders[i]._to_string();
  }

  j = json{
      {"type", orderby->type._to_string()},
      {"input", orderby->input},
      {"orderby_cols", orderby->orderby_cols},
      {"orders", orders},
  };
}

void to_json(json &j, const std::shared_ptr<Expr> &expr) {
  switch (expr->type) {
    case ExprType::ColumnReference:
      j = json(std::dynamic_pointer_cast<ColumnReference>(expr));
      break;
    case ExprType::IntLiteral:
      j = json(std::dynamic_pointer_cast<IntLiteral>(expr));
      break;
    case ExprType::StrLiteral:
      j = json(std::dynamic_pointer_cast<StrLiteral>(expr));
      break;
    case ExprType::Comparative:
      j = json(std::dynamic_pointer_cast<Comparative>(expr));
      break;
    case ExprType::Disjunctive:
      j = json(std::dynamic_pointer_cast<Disjunctive>(expr));
      break;
    case ExprType::Arithmetic:
      j = json(std::dynamic_pointer_cast<Arithmetic>(expr));
      break;
    case ExprType::AggFunc:
      j = json(std::dynamic_pointer_cast<AggFunc>(expr));
      break;
    default:
      break;
  }
}
void to_json(json &j,
             const std::shared_ptr<ColumnReference> &column_reference) {
  j = json{{"type", column_reference->type._to_string()},
           {"column_name", column_reference->column_name},
           {"i_table", column_reference->i_table},
           {"i_column", column_reference->i_column}};
}
void to_json(json &j, const std::shared_ptr<IntLiteral> &int_literal) {
  j = json{
      {"type", int_literal->type._to_string()},
      {"value", int_literal->value},
  };
}
void to_json(json &j, const std::shared_ptr<StrLiteral> &str_literal) {
  j = json{
      {"type", str_literal->type._to_string()},
      {"value", str_literal->value},
  };
}
void to_json(json &j, const std::shared_ptr<Comparative> &comparative) {
  j = json{
      {"type", comparative->type._to_string()},
      {"left", comparative->left},
      {"variant", comparative->op._to_string()},
      {"right", comparative->right},
  };
}
void to_json(json &j, const std::shared_ptr<Disjunctive> &disjunctive) {
  j = json{
      {"type", disjunctive->type._to_string()},
      {"i_table", disjunctive->i_table},
      {"exprs", disjunctive->exprs},
  };
}
void to_json(json &j, const std::shared_ptr<Arithmetic> &arithmetic) {
  j = json{
      {"type", arithmetic->type._to_string()},
      {"left", arithmetic->left},
      {"op", arithmetic->op._to_string()},
      {"right", arithmetic->right},
  };
}
void to_json(json &j, const std::shared_ptr<AggFunc> &aggfunc) {
  j = json{{"type", aggfunc->type._to_string()},
           {"func", aggfunc->func._to_string()},
           {"expr", aggfunc->expr}};
}

}  // namespace resolver
}  // namespace hustle