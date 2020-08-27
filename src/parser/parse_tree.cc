#include "parse_tree.h"

namespace hustle {
namespace parser {

std::string Comparative::get_plan_type() {
  if (left->type == +ExprType::ColumnReference and
      right->type == +ExprType::ColumnReference) {
    return "JOIN_Pred";
  } else if (left->type == +ExprType::ColumnReference and
             right->type != +ExprType::ColumnReference) {
    return "SELECT_Pred";
  } else if (left->type != +ExprType::ColumnReference and
             right->type == +ExprType::ColumnReference) {
    auto temp = left;
    left = right;
    right = temp;
    return "SELECT_Pred";
  } else {
    return "UNKNOWN";
  }
}

void from_json(const json &j, std::shared_ptr<ParseTree> &parse_tree) {
  parse_tree = std::make_shared<ParseTree>(
      j.at("tableList").get<std::vector<std::string>>(),
      j.at("project").get<std::vector<std::shared_ptr<Project>>>(),
      j.at("loop_pred").get<std::vector<std::shared_ptr<LoopPredicate>>>(),
      j.at("other_pred").get<std::vector<std::shared_ptr<Expr>>>(),
      j.at("aggregate").get<std::vector<std::shared_ptr<AggFunc>>>(),
      j.at("group_by").get<std::vector<std::shared_ptr<ColumnReference>>>(),
      j.at("order_by").get<std::vector<std::shared_ptr<OrderBy>>>());
}
void to_json(json &j, const std::shared_ptr<ParseTree> &parse_tree) {
  j = json{{"tableList", parse_tree->tableList},
           {"project", parse_tree->project},
           {"loop_pred", parse_tree->loop_pred},
           {"other_pred", parse_tree->other_pred},
           {"aggregate", parse_tree->aggregate},
           {"group_by", parse_tree->group_by},
           {"order_by", parse_tree->order_by}};
}
void from_json(const json &j, std::shared_ptr<LoopPredicate> &loop_pred) {
  loop_pred =
      std::make_shared<LoopPredicate>(j.at("fromtable"), j.at("predicates"));
}
void to_json(json &j, const std::shared_ptr<LoopPredicate> &loop_pred) {
  j = json{{"fromtable", loop_pred->fromtable},
           {"predicates", loop_pred->predicates}};
}
void from_json(const json &j, std::shared_ptr<Project> &proj) {
  proj = std::make_shared<Project>(j.at("proj_name"), j.at("expr"));
}
void to_json(json &j, const std::shared_ptr<Project> &proj) {
  j = json{{"proj_name", proj->proj_name}, {"expr", proj->expr}};
}
void from_json(const json &j, std::shared_ptr<OrderBy> &order_by) {
  order_by = std::make_shared<OrderBy>(
      OrderByDirection::_from_index(j.at("order")), j.at("expr"));
}
void to_json(json &j, const std::shared_ptr<OrderBy> &order_by) {
  j = json{{"order", order_by->order._to_string()}, {"expr", order_by->expr}};
}
void from_json(const json &j, std::shared_ptr<Expr> &expr) {
  auto type = ExprType::_from_string(j.at("type").get<std::string>().c_str());

  switch (type) {
    case ExprType::ColumnReference:
      expr = j.get<std::shared_ptr<ColumnReference>>();
      break;
    case ExprType::IntLiteral:
      expr = j.get<std::shared_ptr<IntLiteral>>();
      break;
    case ExprType::StrLiteral:
      expr = j.get<std::shared_ptr<StrLiteral>>();
      break;
    case ExprType::Comparative:
      expr = j.get<std::shared_ptr<Comparative>>();
      break;
    case ExprType::Disjunctive:
      expr = j.get<std::shared_ptr<Disjunctive>>();
      break;
    case ExprType::Conjunctive:
      expr = j.get<std::shared_ptr<Conjunctive>>();
      break;
    case ExprType::Arithmetic:
      expr = j.get<std::shared_ptr<Arithmetic>>();
      break;
    case ExprType::AggFunc:
      expr = j.get<std::shared_ptr<AggFunc>>();
      break;
  }
}
void from_json(const json &j, std::shared_ptr<ColumnReference> &c) {
  c = std::make_shared<ColumnReference>(j.at("column_name"), j.at("i_table"),
                                        j.at("i_column"));
}
void from_json(const json &j, std::shared_ptr<IntLiteral> &i) {
  i = std::make_shared<IntLiteral>(j.at("value"));
}
void from_json(const json &j, std::shared_ptr<StrLiteral> &s) {
  s = std::make_shared<StrLiteral>(j.at("value"));
}
void from_json(const json &j, std::shared_ptr<Comparative> &pred) {
  pred = std::make_shared<Comparative>(
      j.at("left"),
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
  pred = std::make_shared<Arithmetic>(
      j.at("left"),
      ArithmeticType::_from_string(j.at("op").get<std::string>().c_str()),
      j.at("right"));
}
void from_json(const json &j, std::shared_ptr<AggFunc> &agg) {
  agg = std::make_shared<AggFunc>(
      AggFuncType::_from_string_nocase(j.at("func").get<std::string>().c_str()),
      j.at("expr"));
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
    case ExprType::Conjunctive:
      j = json(std::dynamic_pointer_cast<Conjunctive>(expr));
      break;
    case ExprType::Arithmetic:
      j = json(std::dynamic_pointer_cast<Arithmetic>(expr));
      break;
    case ExprType::AggFunc:
      j = json(std::dynamic_pointer_cast<AggFunc>(expr));
      break;
  }
}
void to_json(json &j, const std::shared_ptr<ColumnReference> &c) {
  j = json{{"type", c->type._to_string()},
           {"column_name", c->column_name},
           {"i_table", c->i_table},
           {"i_column", c->i_column}};
}
void to_json(json &j, const std::shared_ptr<IntLiteral> &i) {
  j = json{{"type", i->type._to_string()}, {"value", i->value}};
}
void to_json(json &j, const std::shared_ptr<StrLiteral> &s) {
  j = json{{"type", s->type._to_string()}, {"value", s->value}};
}
void to_json(json &j, const std::shared_ptr<Comparative> &pred) {
  j = json{{"type", pred->type._to_string()},
           {"left", pred->left},
           {"op", pred->op._to_string()},
           {"right", pred->right},
           {"plan_type", pred->plan_type}};
}
void to_json(json &j, const std::shared_ptr<Disjunctive> &pred) {
  j = json{
      {"type", pred->type._to_string()},
      {"left", pred->left},
      {"right", pred->right},
  };
}
void to_json(json &j, const std::shared_ptr<Conjunctive> &pred) {
  j = json{
      {"type", pred->type._to_string()},
      {"left", pred->left},
      {"right", pred->right},
  };
}
void to_json(json &j, const std::shared_ptr<Arithmetic> &pred) {
  j = json{
      {"type", pred->type._to_string()},
      {"left", pred->left},
      {"op", pred->op._to_string()},
      {"right", pred->right},
  };
}
void to_json(json &j, const std::shared_ptr<AggFunc> &agg) {
  j = json{
      {"type", agg->type._to_string()},
      {"func", agg->func._to_string()},
      {"expr", agg->expr},
  };
}

}  // namespace parser
}  // namespace hustle