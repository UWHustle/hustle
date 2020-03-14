#ifndef HUSTLE_PLAN_H
#define HUSTLE_PLAN_H

#include <iostream>
#include <vector>

namespace hustle {
namespace resolver {

class Expr {};

class Column : public Expr {
 public:
  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  int value;
};

class StrLiteral : public Expr {
 public:
  std::string value;
};



class QueryOperator {};

class TableReference : public QueryOperator {
 public:
  int i_table;
};

class Select : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::shared_ptr<Expr> filter;
};

class Project : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Expr>> proj_exprs;
  std::vector<std::string> proj_names;
};

class Join : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> left;
  std::shared_ptr<QueryOperator> right;
  std::shared_ptr<Expr> predicate;
};

class GroupBy : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Column>> groupby_cols;
};

class OrderBy : public QueryOperator {
 public:
  std::shared_ptr<QueryOperator> input;
  std::vector<std::shared_ptr<Column>> orderby_cols;
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
