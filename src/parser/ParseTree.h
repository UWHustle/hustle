#ifndef HUSTLE_PARSETREE_H
#define HUSTLE_PARSETREE_H

#include <nlohmann/json.hpp>
#include <utility>

#include "types/types.h"

namespace hustle {
namespace parser {

using nlohmann::json;
using namespace hustle::types;

/**
 * Expr
 */
class Expr {
 public:
  virtual ~Expr() = default;
  Expr(ExprType _type) : type(_type) {}

  ExprType type;
};

class ColumnReference : public Expr {
 public:
  ColumnReference(std::string col_name, int t, int c)
      : Expr(ExprType::ColumnReference), column_name(std::move(col_name)), i_table(t), i_column(c) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  explicit IntLiteral(int v) : Expr(ExprType::IntLiteral), value(v) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  explicit StrLiteral(std::string str) : Expr(ExprType::StrLiteral), value(std::move(str)) {}

  std::string value;
};

class Comparative : public Expr {
 public:
  Comparative(std::shared_ptr<Expr> l,
              ComparativeType _op,
              std::shared_ptr<Expr> r)
      : Expr(ExprType::Comparative),
        left(std::move(l)),
        op(_op),
        right(std::move(r)),
        plan_type(get_plan_type()) {}

  std::shared_ptr<Expr> left;
  ComparativeType op;
  std::shared_ptr<Expr> right;
  std::string plan_type;

 private:
  std::string get_plan_type();
};

std::string Comparative::get_plan_type() {
  if (left->type == +ExprType::ColumnReference and right->type == +ExprType::ColumnReference) {
    return "JOIN_Pred";
  } else if (left->type == +ExprType::ColumnReference and right->type != +ExprType::ColumnReference) {
    return "SELECT_Pred";
  } else if (left->type != +ExprType::ColumnReference and right->type == +ExprType::ColumnReference) {
    auto temp = left;
    left = right;
    right = temp;
    return "SELECT_Pred";
  } else {
    return "UNKNOWN";
  }
}

class Disjunctive : public Expr {
 public:
  Disjunctive(std::shared_ptr<Expr> l, std::shared_ptr<Expr> r)
      : Expr(ExprType::Disjunctive), left(std::move(l)), right(std::move(r)) {}

  std::shared_ptr<Expr> left;
  std::shared_ptr<Expr> right;
};

class Conjunctive : public Expr {
 public:
  Conjunctive(std::shared_ptr<Expr> l, std::shared_ptr<Expr> r)
      : Expr(ExprType::Conjunctive), left(std::move(l)), right(std::move(r)) {}

  std::shared_ptr<Expr> left;
  std::shared_ptr<Expr> right;
};

class Arithmetic : public Expr {
 public:
  Arithmetic(std::shared_ptr<Expr> _left,
             ArithmeticType _op,
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
  AggFunc(std::string f, std::shared_ptr<Expr> e)
      : Expr(ExprType::AggFunc), func(std::move(f)), expr(std::move(e)) {}

  std::string func;
  std::shared_ptr<Expr> expr;
};

/**
 * LoopPredicate
 */
class LoopPredicate {
 public:
  LoopPredicate() {}
  LoopPredicate(int i, std::vector<std::shared_ptr<Comparative>> preds)
      : fromtable(i), predicates(std::move(preds)) {}

  int fromtable;
  std::vector<std::shared_ptr<Comparative>> predicates;
};

/**
 * Project
 */
class Project {
 public:
  Project() {}
  Project(std::string name, std::shared_ptr<Expr> e) : proj_name(std::move(name)), expr(std::move(e)) {}

  std::string proj_name;
  std::shared_ptr<Expr> expr;
};

/**
 * OrderBy
 */
class OrderBy {
 public:
  OrderBy() {}
  OrderBy(int f, std::shared_ptr<Expr> e) : sort_flag(f), expr(std::move(e)) {}

  int sort_flag;
  std::shared_ptr<Expr> expr;
};

/**
 * ParseTree
 */
class ParseTree {
 public:
  ParseTree() : project(), loop_pred(), other_pred() {}
  ParseTree(std::vector<std::shared_ptr<Project>> proj,
            std::vector<std::shared_ptr<LoopPredicate>> &&l_pred,
            std::vector<std::shared_ptr<Expr>> o_pred,
            std::vector<std::shared_ptr<ColumnReference>> group_by,
            std::vector<std::shared_ptr<OrderBy>> order_by)
      : project(std::move(proj)),
        loop_pred(std::move(l_pred)),
        other_pred(std::move(o_pred)),
        group_by(std::move(group_by)),
        order_by(std::move(order_by)) {}

  std::vector<std::shared_ptr<Project>> project;
  std::vector<std::shared_ptr<LoopPredicate>> loop_pred;
  std::vector<std::shared_ptr<Expr>> other_pred;
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

void from_json(const json &j, std::shared_ptr<ParseTree> &parse_tree) {
  parse_tree = std::make_shared<ParseTree>();
  parse_tree->project = j.at("project").get<std::vector<std::shared_ptr<Project>>>();
  parse_tree->loop_pred = j.at("loop_pred").get<std::vector<std::shared_ptr<LoopPredicate>>>();
  parse_tree->other_pred = j.at("other_pred").get<std::vector<std::shared_ptr<Expr>>>();
  parse_tree->group_by = j.at("group_by").get<std::vector<std::shared_ptr<ColumnReference>>>();
  parse_tree->order_by = j.at("order_by").get<std::vector<std::shared_ptr<OrderBy>>>();
}
void to_json(json &j, const std::shared_ptr<ParseTree> &parse_tree) {
  j = json
      {
          {"project", parse_tree->project},
          {"loop_pred", parse_tree->loop_pred},
          {"other_pred", parse_tree->other_pred},
          {"group_by", parse_tree->group_by},
          {"order_by", parse_tree->order_by}
      };
}
void from_json(const json &j, std::shared_ptr<LoopPredicate> &loop_pred) {
  loop_pred = std::make_shared<LoopPredicate>(j.at("fromtable"), j.at("predicates"));
}
void to_json(json &j, const std::shared_ptr<LoopPredicate> &loop_pred) {
  j = json
      {
          {"fromtable", loop_pred->fromtable},
          {"predicates", loop_pred->predicates}
      };
}
void from_json(const json &j, std::shared_ptr<Project> &proj) {
  proj = std::make_shared<Project>(j.at("proj_name"), j.at("expr"));
}
void to_json(json &j, const std::shared_ptr<Project> &proj) {
  j = json
      {
          {"proj_name", proj->proj_name},
          {"expr", proj->expr}
      };
}
void from_json(const json &j, std::shared_ptr<OrderBy> &order_by) {
  order_by = std::make_shared<OrderBy>(j.at("sort_flag"), j.at("expr"));
}
void to_json(json &j, const std::shared_ptr<OrderBy> &order_by) {
  j = json
      {
          {"sort_flag", order_by->sort_flag},
          {"expr", order_by->expr}
      };
}
void from_json(const json &j, std::shared_ptr<Expr> &expr) {
  auto type = ExprType::_from_string(j.at("type").get<std::string>().c_str());

  switch (type) {
    case ExprType::ColumnReference : expr = j.get<std::shared_ptr<ColumnReference>>();
      break;
    case ExprType::IntLiteral : expr = j.get<std::shared_ptr<IntLiteral>>();
      break;
    case ExprType::StrLiteral : expr = j.get<std::shared_ptr<StrLiteral>>();
      break;
    case ExprType::Comparative : expr = j.get<std::shared_ptr<Comparative>>();
      break;
    case ExprType::Disjunctive : expr = j.get<std::shared_ptr<Disjunctive>>();
      break;
    case ExprType::Conjunctive : expr = j.get<std::shared_ptr<Conjunctive>>();
      break;
    case ExprType::Arithmetic : expr = j.get<std::shared_ptr<Arithmetic>>();
      break;
    case ExprType::AggFunc : expr = j.get<std::shared_ptr<AggFunc>>();
      break;
  }
}
void from_json(const json &j, std::shared_ptr<ColumnReference> &c) {
  c = std::make_shared<ColumnReference>(j.at("column_name"), j.at("i_table"), j.at("i_column"));
//  c->i_table = j.at("i_table");
//  c->i_column = j.at("i_column");
//  j.at("i_table").get_to(c->i_table);
//  j.at("i_column").get_to(c->i_column);
}
void from_json(const json &j, std::shared_ptr<IntLiteral> &i) {
  i = std::make_shared<IntLiteral>(j.at("value"));
}
void from_json(const json &j, std::shared_ptr<StrLiteral> &s) {
  s = std::make_shared<StrLiteral>(j.at("value"));
}
void from_json(const json &j, std::shared_ptr<Comparative> &pred) {
  pred = std::make_shared<Comparative>(j.at("left"),
                                       ComparativeType::_from_string(j.at("op").get<std::string>().c_str()),
                                       j.at("right"));
}
void from_json(const json &j, std::shared_ptr<Disjunctive> &pred) {
  pred = std::make_shared<Disjunctive>(j.at("left"), j.at("right"));
}
void from_json(const json &j, std::shared_ptr<Conjunctive> &pred) {
  pred = std::make_shared<Conjunctive>(j.at("left"), j.at("right"));
}
void from_json(const json &j, std::shared_ptr<Arithmetic> &pred) {
  pred = std::make_shared<Arithmetic>(j.at("left"),
                                      ArithmeticType::_from_string(j.at("op").get<std::string>().c_str()),
                                      j.at("right"));
}
void from_json(const json &j, std::shared_ptr<AggFunc> &agg) {
  agg = std::make_shared<AggFunc>(j.at("func"), j.at("expr"));
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
    case ExprType::Conjunctive : j = json(std::dynamic_pointer_cast<Conjunctive>(expr));
      break;
    case ExprType::Arithmetic : j = json(std::dynamic_pointer_cast<Arithmetic>(expr));
      break;
    case ExprType::AggFunc : j = json(std::dynamic_pointer_cast<AggFunc>(expr));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<ColumnReference> &c) {
  j = json
      {
          {"type", c->type._to_string()},
          {"column_name", c->column_name},
          {"i_table", c->i_table},
          {"i_column", c->i_column}
      };
}
void to_json(json &j, const std::shared_ptr<IntLiteral> &i) {
  j = json
      {
          {"type", i->type._to_string()},
          {"value", i->value}
      };
}
void to_json(json &j, const std::shared_ptr<StrLiteral> &s) {
  j = json
      {
          {"type", s->type._to_string()},
          {"value", s->value}
      };
}
void to_json(json &j, const std::shared_ptr<Comparative> &pred) {
  j = json
      {
          {"type", pred->type._to_string()},
          {"left", pred->left},
          {"op", pred->op._to_string()},
          {"right", pred->right},
          {"plan_type", pred->plan_type}
      };
}
void to_json(json &j, const std::shared_ptr<Disjunctive> &pred) {
  j = json
      {
          {"type", pred->type._to_string()},
          {"left", pred->left},
          {"right", pred->right},
      };
}
void to_json(json &j, const std::shared_ptr<Conjunctive> &pred) {
  j = json
      {
          {"type", pred->type._to_string()},
          {"left", pred->left},
          {"right", pred->right},
      };
}
void to_json(json &j, const std::shared_ptr<Arithmetic> &pred) {
  j = json
      {
          {"type", pred->type._to_string()},
          {"left", pred->left},
          {"op", pred->op._to_string()},
          {"right", pred->right},
      };
}
void to_json(json &j, const std::shared_ptr<AggFunc> &agg) {
  j = json
      {
          {"type", agg->type._to_string()},
          {"func", agg->func},
          {"expr", agg->expr},
      };
}

}
}

#endif //HUSTLE_PARSETREE_H
