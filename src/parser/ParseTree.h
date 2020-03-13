#ifndef HUSTLE_PARSETREE_H
#define HUSTLE_PARSETREE_H

#include <nlohmann/json.hpp>
#include <utility>

namespace hustle {
namespace parser {

using nlohmann::json;


/**
 * Expr
 */
class Expr {
public:
  virtual ~Expr() = default;
  Expr() {}
  Expr(std::string&& t) : type(std::move(t)) {}

  std::string type;
};

class Column : public Expr {
public:
  Column(std::string col_name, int t, int c) : Expr("Column"), column_name(std::move(col_name)), i_table(t), i_column(c) {}

  std::string column_name;
  int i_table;
  int i_column;
};

class IntLiteral : public Expr {
 public:
  explicit IntLiteral(int v) : Expr("IntLiteral"), value(v) {}

  int value;
};

class StrLiteral : public Expr {
 public:
  explicit StrLiteral(std::string&& str) : Expr("StrLiteral"), value(std::move(str)) {}

  std::string value;
};

class CompositeExpr : public Expr {
 public:
  CompositeExpr() {}
  CompositeExpr(std::shared_ptr<Expr> l, int o, std::shared_ptr<Expr> r) : Expr("CompositeExpr"), left(std::move(l)), op(o), right(std::move(r)) {}

  std::shared_ptr<Expr> left;
  int op;
  std::shared_ptr<Expr> right;
};

class AggFunc : public Expr {
 public:
  AggFunc() {}
  AggFunc(std::string f, std::shared_ptr<Expr> e) : Expr("AggFunc"), func(std::move(f)), expr(std::move(e)) {}

  std::string func;
  std::shared_ptr<Expr> expr;
};

/**
 * LoopPredicate
 */
class LoopPredicate {
 public:
  LoopPredicate() {}
  LoopPredicate(int i, std::vector<std::shared_ptr<Expr>> preds) : fromtable(i), predicates(std::move(preds)) {}

  int fromtable;
  std::vector<std::shared_ptr<Expr>> predicates;
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
  ParseTree(): project(), loop_pred(), other_pred() {}
  ParseTree(std::vector<std::shared_ptr<Project>> proj,
            std::vector<std::shared_ptr<LoopPredicate>>&& l_pred,
            std::vector<std::shared_ptr<Expr>> o_pred,
            std::vector<std::shared_ptr<Expr>> group_by,
            std::vector<std::shared_ptr<OrderBy>> order_by
            ) : project(std::move(proj)), loop_pred(std::move(l_pred)), other_pred(std::move(o_pred)), group_by(std::move(group_by)), order_by(std::move(order_by)) {}

  std::vector<std::shared_ptr<Project>> project;
  std::vector<std::shared_ptr<LoopPredicate>> loop_pred;
  std::vector<std::shared_ptr<Expr>> other_pred;
  std::vector<std::shared_ptr<Expr>> group_by;
  std::vector<std::shared_ptr<OrderBy>> order_by;
};




void from_json(const json &j, ParseTree &parse_tree);
void to_json(json &j, const ParseTree &parse_tree);

void from_json(const json& j, std::shared_ptr<LoopPredicate>& loop_pred);
void to_json(json& j, const std::shared_ptr<LoopPredicate>& loop_pred);

void from_json(const json& j, std::shared_ptr<Project>& proj);
void to_json(json& j, const std::shared_ptr<Project>& proj);

void from_json(const json& j, std::shared_ptr<OrderBy>& proj);
void to_json(json& j, const std::shared_ptr<OrderBy>& proj);

void from_json(const json& j, std::shared_ptr<Column>& c);
void from_json(const json& j, std::shared_ptr<IntLiteral>& i);
void from_json(const json& j, std::shared_ptr<StrLiteral>& s);
void from_json(const json& j, std::shared_ptr<CompositeExpr>& pred);
void from_json(const json& j, std::shared_ptr<AggFunc>& agg);
void from_json(const json& j, std::shared_ptr<Expr>& expr);
void to_json(json& j, const std::shared_ptr<Column>& c);
void to_json(json& j, const std::shared_ptr<IntLiteral>& i);
void to_json(json& j, const std::shared_ptr<StrLiteral>& s);
void to_json(json& j, const std::shared_ptr<Expr>& expr);
void to_json(json& j, const std::shared_ptr<CompositeExpr>& pred);
void to_json(json& j, const std::shared_ptr<AggFunc>& agg);




void from_json(const json &j, std::shared_ptr<Column> &c) {
  c = std::make_shared<Column>(j.at("column_name"), j.at("i_table"), j.at("i_column"));
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
void from_json(const json &j, std::shared_ptr<CompositeExpr> &pred) {
  pred = std::make_shared<CompositeExpr>(j.at("left"), j.at("op"), j.at("right"));
}
void from_json(const json &j, std::shared_ptr<AggFunc> &agg) {
  agg = std::make_shared<AggFunc>(j.at("func"), j.at("expr"));
}
void from_json(const json &j, std::shared_ptr<Expr> &expr) {
  auto type = j.at("type").get<std::string>();

  if(type == "Column") {
    expr = j.get<std::shared_ptr<Column>>();
  } else if (type == "IntLiteral") {
    expr = j.get<std::shared_ptr<IntLiteral>>();
  } else if (type == "StrLiteral") {
    expr = j.get<std::shared_ptr<StrLiteral>>();
  } else if (type == "CompositeExpr") {
    expr = j.get<std::shared_ptr<CompositeExpr>>();
  } else if (type == "AggFunc") {
    expr = j.get<std::shared_ptr<AggFunc>>();
  }
}
void to_json(json &j, const std::shared_ptr<Column> &c) {
  j = json
      {
          {"type", c->type},
          {"column_name", c->column_name},
          {"i_table", c->i_table},
          {"i_column", c->i_column}
      };
}
void to_json(json &j, const std::shared_ptr<IntLiteral> &i) {
  j = json
      {
          {"type", i->type},
          {"value", i->value}
      };
}
void to_json(json &j, const std::shared_ptr<StrLiteral> &s) {
  j = json
      {
          {"type", s->type},
          {"value", s->value}
      };
}
void to_json(json &j, const std::shared_ptr<Expr> &expr) {
  if(expr->type == "Column") {
    j = json(std::dynamic_pointer_cast<Column>(expr));
  } else if (expr->type == "IntLiteral") {
    j = json(std::dynamic_pointer_cast<IntLiteral>(expr));
  } else if (expr->type == "StrLiteral") {
    j = json(std::dynamic_pointer_cast<StrLiteral>(expr));
  } else if (expr->type == "CompositeExpr") {
    j = json(std::dynamic_pointer_cast<CompositeExpr>(expr));
  } else if (expr->type == "AggFunc") {
    j = json(std::dynamic_pointer_cast<AggFunc>(expr));
  }
}
void to_json(json &j, const std::shared_ptr<CompositeExpr> &pred) {
  j = json
      {
          {"type", pred->type},
          {"left", pred->left},
          {"op", pred->op},
          {"right", pred->right}
      };
}
void to_json(json &j, const std::shared_ptr<AggFunc> &agg) {
  j = json
      {
          {"type", agg->type},
          {"func", agg->func},
          {"expr", agg->expr},
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
void from_json(const json &j, ParseTree &parse_tree) {
  parse_tree.project = j.at("project").get<std::vector<std::shared_ptr<Project>>>();
  parse_tree.loop_pred = j.at("loop_pred").get<std::vector<std::shared_ptr<LoopPredicate>>>();
  parse_tree.other_pred = j.at("other_pred").get<std::vector<std::shared_ptr<Expr>>>();
  parse_tree.group_by = j.at("group_by").get<std::vector<std::shared_ptr<Expr>>>();
  parse_tree.order_by = j.at("order_by").get<std::vector<std::shared_ptr<OrderBy>>>();
}
void to_json(json &j, const ParseTree &parse_tree) {
  j = json
      {
          {"project", parse_tree.project},
          {"loop_pred", parse_tree.loop_pred},
          {"other_pred", parse_tree.other_pred},
          {"group_by", parse_tree.group_by},
          {"order_by", parse_tree.order_by}
      };
}

}
}


#endif //HUSTLE_PARSETREE_H
