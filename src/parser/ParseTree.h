#ifndef HUSTLE_PARSETREE_H
#define HUSTLE_PARSETREE_H

#include <nlohmann/json.hpp>
#include <utility>

#include "types/Types.h"

namespace hustle {
namespace parser {

using nlohmann::json;
using namespace hustle::types;

/**
 * abstract class: Expr
 * derived class: ColumnReference, IntLiteral, StrLiteral, Comparative,
 *                Disjunctive, Arithmetic, AggFunc
 */
class Expr {
 public:
  virtual ~Expr() = default;
  Expr(ExprType _type) : type(_type) {}

  ExprType type;
};

class ColumnReference : public Expr {
 public:
  ColumnReference(std::string _column_name,
                  int _i_table,
                  int _i_column) : Expr(ExprType::ColumnReference),
                                   column_name(std::move(_column_name)),
                                   i_table(_i_table),
                                   i_column(_i_column) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  explicit IntLiteral(int _value) : Expr(ExprType::IntLiteral),
                                    value(_value) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  explicit StrLiteral(std::string _value) : Expr(ExprType::StrLiteral),
                                            value(std::move(_value)) {}

  std::string value;
};

class Comparative : public Expr {
 public:
  Comparative(std::shared_ptr<Expr> _left,
              ComparativeType _op,
              std::shared_ptr<Expr> _right) : Expr(ExprType::Comparative),
                                              left(std::move(_left)),
                                              op(_op),
                                              right(std::move(_right)),
                                              plan_type(get_plan_type()) {}

  std::shared_ptr<Expr> left;
  ComparativeType op;
  std::shared_ptr<Expr> right;
  std::string plan_type;

 private:
  std::string get_plan_type();
};



class Disjunctive : public Expr {
 public:
  Disjunctive(std::shared_ptr<Expr> _left,
              std::shared_ptr<Expr> _right) : Expr(ExprType::Disjunctive),
                                              left(std::move(_left)),
                                              right(std::move(_right)) {}

  std::shared_ptr<Expr> left;
  std::shared_ptr<Expr> right;
};

class Conjunctive : public Expr {
 public:
  Conjunctive(std::shared_ptr<Expr> _left,
              std::shared_ptr<Expr> _right) : Expr(ExprType::Conjunctive),
                                              left(std::move(_left)),
                                              right(std::move(_right)) {}

  std::shared_ptr<Expr> left;
  std::shared_ptr<Expr> right;
};

class Arithmetic : public Expr {
 public:
  Arithmetic(std::shared_ptr<Expr> _left,
             ArithmeticType _op,
             std::shared_ptr<Expr> _right) : Expr(ExprType::Arithmetic),
                                             left(std::move(_left)),
                                             op(_op),
                                             right(std::move(_right)) {}

  std::shared_ptr<Expr> left;
  ArithmeticType op;
  std::shared_ptr<Expr> right;
};

class AggFunc : public Expr {
 public:
  AggFunc(AggFuncType _func,
          std::shared_ptr<Expr> _expr) : Expr(ExprType::AggFunc),
                                         func(_func),
                                         expr(std::move(_expr)) {}

  AggFuncType func;
  std::shared_ptr<Expr> expr;
};

/**
 * LoopPredicate
 */
class LoopPredicate {
 public:
  LoopPredicate(int _fromtable,
                std::vector<std::shared_ptr<Comparative>> _predicates
  ) : fromtable(_fromtable),
      predicates(std::move(_predicates)) {}

  int fromtable;
  std::vector<std::shared_ptr<Comparative>> predicates;
};

/**
 * Project
 */
class Project {
 public:
  Project(std::string _proj_name,
          std::shared_ptr<Expr> _expr) : proj_name(std::move(_proj_name)),
                                         expr(std::move(_expr)) {}

  std::string proj_name;
  std::shared_ptr<Expr> expr;
};

/**
 * OrderBy
 */
class OrderBy {
 public:
  OrderBy(OrderByDirection _order,
          std::shared_ptr<Expr> _expr)
      : order(_order),
        expr(std::move(_expr)) {}

  OrderByDirection order;
  std::shared_ptr<Expr> expr;
};

/**
 * ParseTree
 */
class ParseTree {
 public:
  ParseTree(std::vector<std::string> _tableList,
            std::vector<std::shared_ptr<Project>> _project,
            std::vector<std::shared_ptr<LoopPredicate>> _loop_pred,
            std::vector<std::shared_ptr<Expr>> _other_pred,
            std::vector<std::shared_ptr<AggFunc>> _aggregate,
            std::vector<std::shared_ptr<ColumnReference>> _group_by,
            std::vector<std::shared_ptr<OrderBy>> _order_by
  ) : tableList(std::move(_tableList)),
      project(std::move(_project)),
      loop_pred(std::move(_loop_pred)),
      other_pred(std::move(_other_pred)),
      aggregate(std::move(_aggregate)),
      group_by(std::move(_group_by)),
      order_by(std::move(_order_by)) {}

  std::vector<std::string> tableList;
  std::vector<std::shared_ptr<Project>> project;
  std::vector<std::shared_ptr<LoopPredicate>> loop_pred;
  std::vector<std::shared_ptr<Expr>> other_pred;
  std::vector<std::shared_ptr<AggFunc>> aggregate;
  std::vector<std::shared_ptr<ColumnReference>> group_by;
  std::vector<std::shared_ptr<OrderBy>> order_by;
};

void from_json(const json &j, std::shared_ptr<ParseTree> &parse_tree);
void to_json(json &j, const std::shared_ptr<ParseTree> &parse_tree);

void from_json(const json &j, std::shared_ptr<LoopPredicate> &loop_pred);
void to_json(json &j, const std::shared_ptr<LoopPredicate> &loop_pred);

void from_json(const json &j, std::shared_ptr<Project> &proj);
void to_json(json &j, const std::shared_ptr<Project> &proj);

void from_json(const json &j, std::shared_ptr<OrderBy> &order_by);
void to_json(json &j, const std::shared_ptr<OrderBy> &order_by);

void from_json(const json &j, std::shared_ptr<Expr> &expr);
void from_json(const json &j, std::shared_ptr<ColumnReference> &c);
void from_json(const json &j, std::shared_ptr<IntLiteral> &i);
void from_json(const json &j, std::shared_ptr<StrLiteral> &s);
void from_json(const json &j, std::shared_ptr<Comparative> &pred);
void from_json(const json &j, std::shared_ptr<Disjunctive> &pred);
void from_json(const json &j, std::shared_ptr<Conjunctive> &pred);
void from_json(const json &j, std::shared_ptr<Arithmetic> &pred);
void from_json(const json &j, std::shared_ptr<AggFunc> &agg);
void to_json(json &j, const std::shared_ptr<Expr> &expr);
void to_json(json &j, const std::shared_ptr<ColumnReference> &c);
void to_json(json &j, const std::shared_ptr<IntLiteral> &i);
void to_json(json &j, const std::shared_ptr<StrLiteral> &s);
void to_json(json &j, const std::shared_ptr<Comparative> &pred);
void to_json(json &j, const std::shared_ptr<Disjunctive> &pred);
void to_json(json &j, const std::shared_ptr<Conjunctive> &pred);
void to_json(json &j, const std::shared_ptr<Arithmetic> &pred);
void to_json(json &j, const std::shared_ptr<AggFunc> &agg);

}
}

#endif //HUSTLE_PARSETREE_H
