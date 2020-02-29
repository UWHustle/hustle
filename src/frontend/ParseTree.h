#ifndef HUSTLE_PARSETREE_H
#define HUSTLE_PARSETREE_H

#include <nlohmann/json.hpp>
#include <utility>

namespace hustle {
namespace frontend {

using nlohmann::json;

class Expr {
public:
  virtual ~Expr() = default;
  Expr() {}
  Expr(std::string&& t) : type(std::move(t)) {}

  std::string type;
};

class Column : public Expr {
public:
  Column(int t, int c) : Expr("COLUMN"), i_table(t), i_column(c) {}

  int i_table;
  int i_column;
};

class Integer : public Expr {
 public:
  explicit Integer(int v) : Expr("INTEGER"), value(v) {}

  int value;
};

void from_json(const json& j, std::shared_ptr<Column>& c)
{
  c = std::make_shared<Column>(j.at("i_table"), j.at("i_column"));

//  c->i_table = j.at("i_table");
//  c->i_column = j.at("i_column");
//  j.at("i_table").get_to(c->i_table);
//  j.at("i_column").get_to(c->i_column);
}

void from_json(const json& j, std::shared_ptr<Integer>& i)
{
  i = std::make_shared<Integer>(j.at("value"));
//  i->set_type("INTEGER");
//  j.at("value").get_to(i->value);
//  i->value = j.at("value");
}

void from_json(const json& j, std::shared_ptr<Expr>& expr)
{
  auto type = j.at("type").get<std::string>();

  if(type == "COLUMN") {
    expr = j.get<std::shared_ptr<Column>>();
  } else if (type == "INTEGER") {
    expr = j.get<std::shared_ptr<Integer>>();
  }
//  expr->set_type(std::move(type));
}

void to_json(json& j, const std::shared_ptr<Column>& c) {
  j = json
      {
          {"type", c->type},
          {"i_table", c->i_table},
          {"i_column", c->i_column}
      };
}

void to_json(json& j, const std::shared_ptr<Integer>& i) {
  j = json
      {
          {"type", i->type},
          {"value", i->value}
      };
}

void to_json(json& j, const std::shared_ptr<Expr>& expr) {
  if(expr->type == "COLUMN") {
    j = json(std::dynamic_pointer_cast<Column>(expr));
  } else if (expr->type == "INTEGER") {
    j = json(std::dynamic_pointer_cast<Integer>(expr));
  }
}


class Predicate {
 public:
  Predicate() {}
  Predicate(std::shared_ptr<Expr> l, int o, std::shared_ptr<Expr> r) : left(std::move(l)), op(o), right(std::move(r)) {}

  std::shared_ptr<Expr> left;
  int op;
  std::shared_ptr<Expr> right;
};

void from_json(const json& j, Predicate& pred)
{
  pred.left = j.at("left");
  pred.op = j.at("op");
  pred.right = j.at("right");
}

void to_json(json& j, const Predicate& pred) {
  j = json
      {
          {"left", pred.left},
          {"op", pred.op},
          {"right", pred.right}
      };
}

class LoopPredicate {
 public:
  LoopPredicate() {}
  LoopPredicate(int i, std::vector<Predicate>&& preds) : fromtable(i), predicates(std::move(preds)) {}

  int fromtable;
  std::vector<Predicate> predicates;
};

void from_json(const json& j, LoopPredicate& loop_pred)
{
  loop_pred.fromtable = j.at("fromtable");
  loop_pred.predicates = j.at("predicates").get<std::vector<Predicate>>();
}

void to_json(json& j, const LoopPredicate& loop_pred) {
  j = json
      {
          {"fromtable", loop_pred.fromtable},
          {"predicates", loop_pred.predicates}
      };
}

class ParseTree {
 public:
  ParseTree(): project(), loop_pred(), other_pred() {}
  ParseTree(std::vector<std::string>&& proj,
            std::vector<LoopPredicate>&& l_pred,
            std::vector<Predicate>&& o_pred
            ) : project(std::move(proj)), loop_pred(std::move(l_pred)), other_pred(std::move(o_pred)) {}

  std::vector<std::string> project;
  std::vector<LoopPredicate> loop_pred;
  std::vector<Predicate> other_pred;
};

void from_json(const json& j, ParseTree& parse_tree)
{
  parse_tree.project = j.at("project").get<std::vector<std::string>>();
  parse_tree.loop_pred = j.at("loop_pred").get<std::vector<LoopPredicate>>();
  parse_tree.other_pred = j.at("other_pred").get<std::vector<Predicate>>();
}

void to_json(json& j, const ParseTree& parse_tree) {
  j = json
      {
          {"project", parse_tree.project},
          {"loop_pred", parse_tree.loop_pred},
          {"other_pred", parse_tree.other_pred}
      };
}

}
}


#endif //HUSTLE_PARSETREE_H
