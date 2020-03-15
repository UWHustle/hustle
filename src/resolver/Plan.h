#ifndef HUSTLE_PLAN_H
#define HUSTLE_PLAN_H

#include <iostream>
#include <vector>

namespace hustle {
namespace resolver {

enum ComparativeVariant {
  NE = 52,
  EQ = 53,
  GT = 54,
  LE = 55,
  LT = 56,
  GE = 57,
};

enum SelectVariant {
  OR = 43,
  AND = 44
};


class Expr {
 public:
  virtual ~Expr() = default;
};

class ColumnReference : public Expr {
 public:
  ColumnReference(std::string column_name_, int i_tab, int i_col) : column_name(std::move(column_name_)), i_table(i_tab), i_column(i_col) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  IntLiteral(int v) : value(v) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  StrLiteral(std::string v) : value(std::move(v)) {}

  std::string value;
};

class Comparative : public Expr {
 public:
  Comparative(std::shared_ptr<ColumnReference> _left,
      int op,
      std::shared_ptr<Expr> _right) : left(std::move(_left)), variant(static_cast<ComparativeVariant>(op)), right(std::move(_right)) {}

  std::shared_ptr<ColumnReference> left;
  ComparativeVariant variant;
  std::shared_ptr<Expr> right;
};

// class Conjunctive : public Expr {
//  public:
//   Conjunctive(std::vector<std::shared_ptr<Comparative>> _exprs) : exprs(std::move(_exprs)) {}
//
//   std::vector<std::shared_ptr<Comparative>> exprs;
// };
//
// class Disjunctive : public Expr {
//  public:
//   Disjunctive(std::vector<std::shared_ptr<Comparative>> _exprs) : exprs(std::move(_exprs)) {}
//
//   std::vector<std::shared_ptr<Comparative>> exprs;
// };


class QueryOperator {
 public:
  virtual ~QueryOperator() = default;
};

class TableReference : public QueryOperator {
 public:
  TableReference(int i) : i_table(i) {}

  int i_table;
};

class Select : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Comparative>> and_filter;
  std::vector<std::shared_ptr<Comparative>> or_filter;
};

class Project : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Expr>> proj_exprs;
  std::vector<std::string> proj_names;
};

class Join : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> left_input;
  std::shared_ptr<QueryOperator> right_input;
  std::shared_ptr<Comparative> join_pred;
};

class GroupBy : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<ColumnReference>> groupby_cols;
};

class OrderBy : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<ColumnReference>> orderby_cols;
  std::vector<bool> order; // 0 : asc, 1 : desc
};

class Plan {};

class Query : public Plan {
 public:
  std::shared_ptr<QueryOperator> query_operator;
};

class Create : public Plan {
  /// TODO(Lichengxi): add Create class
};

}
}

#endif //HUSTLE_PLAN_H
