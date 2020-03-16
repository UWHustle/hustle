#ifndef HUSTLE_PLAN_H
#define HUSTLE_PLAN_H

#include <iostream>
#include <vector>

#include <nlohmann/json.hpp>

#include "types/types.h"

namespace hustle {
namespace resolver {

using nlohmann::json;
using namespace hustle::types;

class Expr {
 public:
  Expr(ExprType _type) : type(_type) {}
  virtual ~Expr() = default;

  ExprType type;
};

class ColumnReference : public Expr {
 public:
  ColumnReference(std::string column_name_,
                  int i_tab,
                  int i_col)
      : Expr(ExprType::ColumnReference),
        column_name(std::move(column_name_)),
        i_table(i_tab),
        i_column(i_col) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  IntLiteral(int v) : Expr(ExprType::IntLiteral), value(v) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  StrLiteral(std::string v) : Expr(ExprType::StrLiteral), value(std::move(v)) {}

  std::string value;
};

class Comparative : public Expr {
 public:
  Comparative(std::shared_ptr<ColumnReference> _left,
              ComparativeType _op,
              std::shared_ptr<Expr> _right)
      : Expr(ExprType::Comparative),
        left(std::move(_left)),
        op(_op),
        right(std::move(_right)) {}

  std::shared_ptr<ColumnReference> left;
  ComparativeType op;
  std::shared_ptr<Expr> right;
};

class Disjunctive : public Expr {
 public:
  Disjunctive(int _i_table, std::vector<std::shared_ptr<Comparative>> _exprs)
      : Expr(ExprType::Disjunctive),
        i_table(_i_table),
        exprs(std::move(_exprs)) {}

  int i_table;
  std::vector<std::shared_ptr<Comparative>> exprs;
};

class QueryOperator {
 public:
  QueryOperator(QueryOperatorType _type) : type(_type) {}
  virtual ~QueryOperator() = default;

  QueryOperatorType type;
};

class TableReference : public QueryOperator {
 public:
  TableReference(int i) : QueryOperator(QueryOperatorType::TableReference), i_table(i) {}

  int i_table;
};

class Select : public QueryOperator {
 public:

  Select(std::shared_ptr<QueryOperator> _input)
      : QueryOperator(QueryOperatorType::Select),
        input(std::move(_input)) {}

  Select(std::shared_ptr<QueryOperator> _input,
         std::vector<std::shared_ptr<Expr>> _filter)
      : QueryOperator(QueryOperatorType::Select),
        input(std::move(_input)),
        filter(std::move(_filter)) {}

  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Expr>> filter;
};

class Project : public QueryOperator {
 public:
  Project(std::shared_ptr<QueryOperator> _input,
          std::vector<std::shared_ptr<Expr>> _proj_exprs,
          std::vector<std::string> _proj_names)
      : QueryOperator(QueryOperatorType::Project),
        input(std::move(_input)),
        proj_exprs(std::move(_proj_exprs)),
        proj_names(std::move(_proj_names)) {}

  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Expr>> proj_exprs;
  std::vector<std::string> proj_names;
};

class Join : public QueryOperator {
 public:
  Join(std::shared_ptr<QueryOperator> _left,
       std::shared_ptr<QueryOperator> _right,
       std::vector<std::shared_ptr<Comparative>> _pred)
      : QueryOperator(QueryOperatorType::Join),
        left_input(std::move(_left)),
        right_input(std::move(_right)),
        join_pred(std::move(_pred)) {}

  std::shared_ptr<QueryOperator> left_input;
  std::shared_ptr<QueryOperator> right_input;
  std::vector<std::shared_ptr<Comparative>> join_pred;
};

class GroupBy : public QueryOperator {
 public:
  GroupBy() : QueryOperator(QueryOperatorType::GroupBy) {}
  GroupBy(std::shared_ptr<QueryOperator> _input,
          std::vector<std::shared_ptr<ColumnReference>> _groupby_cols)
      : QueryOperator(QueryOperatorType::GroupBy),
        input(std::move(_input)),
        groupby_cols(std::move(_groupby_cols)) {}

  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<ColumnReference>> groupby_cols;
};

class OrderBy : public QueryOperator {
 public:
  OrderBy() : QueryOperator(QueryOperatorType::OrderBy) {}

  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<ColumnReference>> orderby_cols;
  std::vector<bool> order; // 0 : asc, 1 : desc
};

class Plan {
 public:
  Plan(PlanType _type) : type(_type) {}
  virtual ~Plan() = default;

  PlanType type;
};

class Query : public Plan {
 public:
  Query(std::shared_ptr<QueryOperator> _query_operator)
      : Plan(PlanType::Query),
        query_operator(std::move(_query_operator)) {}

  std::shared_ptr<QueryOperator> query_operator;
};

class Create : public Plan {
 public:
  Create() : Plan(PlanType::Create) {}

  /// TODO(Lichengxi): add Create class

};

void to_json(json &j, const std::shared_ptr<Plan> &plan);
void to_json(json &j, const std::shared_ptr<Query> &query);
void to_json(json &j, const std::shared_ptr<Create> &create);

void to_json(json &j, const std::shared_ptr<QueryOperator> &query_operator);
void to_json(json &j, const std::shared_ptr<TableReference> &table_reference);
void to_json(json &j, const std::shared_ptr<Select> &select);
void to_json(json &j, const std::shared_ptr<Project> &project);
void to_json(json &j, const std::shared_ptr<Join> &join);
void to_json(json &j, const std::shared_ptr<GroupBy> &groupby);
void to_json(json &j, const std::shared_ptr<OrderBy> &orderby);

void to_json(json &j, const std::shared_ptr<Expr> &expr);
void to_json(json &j, const std::shared_ptr<ColumnReference> &column_reference);
void to_json(json &j, const std::shared_ptr<IntLiteral> &int_literal);
void to_json(json &j, const std::shared_ptr<StrLiteral> &str_literal);
void to_json(json &j, const std::shared_ptr<Comparative> &comparative);
void to_json(json &j, const std::shared_ptr<Disjunctive> &disjunctive);

void to_json(json &j, const std::shared_ptr<Plan> &plan) {
  switch (plan->type) {
    case PlanType::Query : j = json(std::dynamic_pointer_cast<Query>(plan));
      break;
    case PlanType::Create : j = json(std::dynamic_pointer_cast<Create>(plan));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<Query> &query) {
  j = json
      {
          {"type", query->type._to_string()},
          {"query_operator", query->query_operator}
      };
}
void to_json(json &j, const std::shared_ptr<Create> &create) {
  /// TODO(Lichengxi): serialization
}

void to_json(json &j, const std::shared_ptr<QueryOperator> &query_operator) {
  switch (query_operator->type) {
    case QueryOperatorType::TableReference : j = json(std::dynamic_pointer_cast<TableReference>(query_operator));
      break;
    case QueryOperatorType::Select : j = json(std::dynamic_pointer_cast<Select>(query_operator));
      break;
    case QueryOperatorType::Project : j = json(std::dynamic_pointer_cast<Project>(query_operator));
      break;
    case QueryOperatorType::Join : j = json(std::dynamic_pointer_cast<Join>(query_operator));
      break;
    case QueryOperatorType::GroupBy : j = json(std::dynamic_pointer_cast<GroupBy>(query_operator));
      break;
    case QueryOperatorType::OrderBy : j = json(std::dynamic_pointer_cast<OrderBy>(query_operator));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<TableReference> &table_reference) {
  j = json
      {
          {"type", table_reference->type._to_string()},
          {"i_table", table_reference->i_table},
      };
}
void to_json(json &j, const std::shared_ptr<Select> &select) {
  j = json
      {
          {"type", select->type._to_string()},
          {"input", select->input},
          {"filter", select->filter},
      };
}
void to_json(json &j, const std::shared_ptr<Project> &project) {
  j = json
      {
          {"type", project->type._to_string()},
          {"input", project->input},
          {"proj_exprs", project->proj_exprs},
          {"proj_names", project->proj_names},
      };
}
void to_json(json &j, const std::shared_ptr<Join> &join) {
  j = json
      {
          {"type", join->type._to_string()},
          {"left_input", join->left_input},
          {"right_input", join->right_input},
          {"join_pred", join->join_pred},
      };
}
void to_json(json &j, const std::shared_ptr<GroupBy> &groupby) {
  j = json
      {
          {"type", groupby->type._to_string()},
          {"input", groupby->input},
          {"groupby_cols", groupby->groupby_cols},
      };
}
void to_json(json &j, const std::shared_ptr<OrderBy> &orderby) {
  j = json
      {
          {"type", orderby->type._to_string()},
          {"input", orderby->input},
          {"orderby_cols", orderby->orderby_cols},
          {"order", orderby->order},
      };
}

void to_json(json &j, const std::shared_ptr<Expr> &expr) {
  switch (expr->type) {
    case ExprType::ColumnReference : j = json(std::dynamic_pointer_cast<ColumnReference>(expr));
      break;
    case ExprType::IntLiteral : j = json(std::dynamic_pointer_cast<IntLiteral>(expr));
      break;
    case ExprType::StrLiteral : j = json(std::dynamic_pointer_cast<StrLiteral>(expr));
      break;
    case ExprType::Comparative : j = json(std::dynamic_pointer_cast<Comparative>(expr));
      break;
    case ExprType::Disjunctive : j = json(std::dynamic_pointer_cast<Disjunctive>(expr));
      break;
    default:break;
  }
}
void to_json(json &j, const std::shared_ptr<ColumnReference> &column_reference) {
  j = json
      {
          {"type", column_reference->type._to_string()},
          {"column_name", column_reference->column_name},
          {"i_table", column_reference->i_table},
          {"i_column", column_reference->i_column}
      };
}
void to_json(json &j, const std::shared_ptr<IntLiteral> &int_literal) {
  j = json
      {
          {"type", int_literal->type._to_string()},
          {"value", int_literal->value},
      };
}
void to_json(json &j, const std::shared_ptr<StrLiteral> &str_literal) {
  j = json
      {
          {"type", str_literal->type._to_string()},
          {"value", str_literal->value},
      };
}
void to_json(json &j, const std::shared_ptr<Comparative> &comparative) {
  j = json
      {
          {"type", comparative->type._to_string()},
          {"left", comparative->left},
          {"variant", comparative->op._to_string()},
          {"right", comparative->right},
      };
}
void to_json(json &j, const std::shared_ptr<Disjunctive> &disjunctive) {
  j = json
      {
          {"type", disjunctive->type._to_string()},
          {"i_table", disjunctive->i_table},
          {"exprs", disjunctive->exprs},
      };
}

}
}

#endif //HUSTLE_PLAN_H
