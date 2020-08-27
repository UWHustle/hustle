#ifndef HUSTLE_PLAN_H
#define HUSTLE_PLAN_H

#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>

#include "types/types.h"

namespace hustle {
namespace resolver {

using nlohmann::json;
using namespace hustle::types;

/**
 * abstract class: Expr
 * derived class: ColumnReference, IntLiteral, StrLiteral, Comparative,
 *                Disjunctive, Arithmetic, AggFunc
 */
class Expr {
 public:
  explicit Expr(ExprType _type) : type(_type) {}
  virtual ~Expr() = default;

  ExprType type;
};

class ColumnReference : public Expr {
 public:
  ColumnReference(std::string _column_name, int _i_table, int _i_column)
      : Expr(ExprType::ColumnReference),
        column_name(std::move(_column_name)),
        i_table(_i_table),
        i_column(_i_column) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  IntLiteral(int _value) : Expr(ExprType::IntLiteral), value(_value) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  StrLiteral(std::string _value)
      : Expr(ExprType::StrLiteral), value(std::move(_value)) {}

  std::string value;
};

class Comparative : public Expr {
 public:
  Comparative(std::shared_ptr<ColumnReference> _left, ComparativeType _op,
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

class Arithmetic : public Expr {
 public:
  Arithmetic(std::shared_ptr<Expr> _left, ArithmeticType _op,
             std::shared_ptr<Expr> _right)
      : Expr(ExprType::Arithmetic),
        left(std::move(_left)),
        op(_op),
        right(std::move(_right)) {}

  std::shared_ptr<Expr> left;
  ArithmeticType op;
  std::shared_ptr<Expr> right;
};

class AggFunc : public Expr {
 public:
  AggFunc(AggFuncType _func, std::shared_ptr<Expr> _expr)
      : Expr(ExprType::AggFunc), func(_func), expr(std::move(_expr)) {}

  AggFuncType func;
  std::shared_ptr<Expr> expr;
};

/**
 * abstract class: QueryOperator
 * derived class: TableReference, Select, Project, Join, Aggregate, OrderBy
 */
class QueryOperator {
 public:
  QueryOperator(QueryOperatorType _type) : type(_type) {}
  virtual ~QueryOperator() = default;

  QueryOperatorType type;
};

class TableReference : public QueryOperator {
 public:
  TableReference(int i, std::string _table_name)
      : QueryOperator(QueryOperatorType::TableReference),
        i_table(i),
        table_name(std::move(_table_name)) {}

  int i_table;
  std::string table_name;
};

class Select : public QueryOperator {
 public:
  Select(std::shared_ptr<QueryOperator> _input)
      : QueryOperator(QueryOperatorType::Select), input(std::move(_input)) {}

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

class Aggregate : public QueryOperator {
 public:
  Aggregate(std::shared_ptr<QueryOperator> _input,
            std::shared_ptr<AggFunc> _aggregate_func,
            std::vector<std::shared_ptr<ColumnReference>> _groupby_cols)
      : QueryOperator(QueryOperatorType::Aggregate),
        input(std::move(_input)),
        aggregate_func(std::move(_aggregate_func)),
        groupby_cols(std::move(_groupby_cols)) {}

  std::shared_ptr<QueryOperator> input;
  std::shared_ptr<AggFunc> aggregate_func;
  std::vector<std::shared_ptr<ColumnReference>> groupby_cols;
};

class OrderBy : public QueryOperator {
 public:
  OrderBy(std::shared_ptr<QueryOperator> _input,
          std::vector<std::shared_ptr<Expr>> _orderby_cols,
          std::vector<OrderByDirection> _orders)
      : QueryOperator(QueryOperatorType::OrderBy),
        input(std::move(_input)),
        orderby_cols(std::move(_orderby_cols)),
        orders(std::move(_orders)) {}

  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Expr>> orderby_cols;
  std::vector<OrderByDirection> orders;
};

/**
 * abstract class: Plan
 * derived class: Query, Create ...
 */
class Plan {
 public:
  Plan(PlanType _type) : type(_type) {}
  virtual ~Plan() = default;

  PlanType type;
};

class Query : public Plan {
 public:
  Query(std::shared_ptr<QueryOperator> _query_operator)
      : Plan(PlanType::Query), query_operator(std::move(_query_operator)) {}

  std::shared_ptr<QueryOperator> query_operator;
};

class Create : public Plan {
 public:
  Create() : Plan(PlanType::Create) {}

  /// TODO(Lichengxi): add Create class
};

/// TODO(Lichengxi): add more classes derived from Plan

void to_json(json &j, const std::shared_ptr<Plan> &plan);
void to_json(json &j, const std::shared_ptr<Query> &query);
void to_json(json &j, const std::shared_ptr<Create> &create);

void to_json(json &j, const std::shared_ptr<QueryOperator> &query_operator);
void to_json(json &j, const std::shared_ptr<TableReference> &table_reference);
void to_json(json &j, const std::shared_ptr<Select> &select);
void to_json(json &j, const std::shared_ptr<Project> &project);
void to_json(json &j, const std::shared_ptr<Join> &join);

void to_json(json &j, const std::shared_ptr<Aggregate> &aggregate);
void to_json(json &j, const std::shared_ptr<OrderBy> &orderby);

void to_json(json &j, const std::shared_ptr<Expr> &expr);
void to_json(json &j, const std::shared_ptr<ColumnReference> &column_reference);
void to_json(json &j, const std::shared_ptr<IntLiteral> &int_literal);
void to_json(json &j, const std::shared_ptr<StrLiteral> &str_literal);
void to_json(json &j, const std::shared_ptr<Comparative> &comparative);
void to_json(json &j, const std::shared_ptr<Disjunctive> &disjunctive);
void to_json(json &j, const std::shared_ptr<Arithmetic> &arithmetic);
void to_json(json &j, const std::shared_ptr<AggFunc> &aggfunc);
}  // namespace resolver
}  // namespace hustle

#endif  // HUSTLE_PLAN_H
