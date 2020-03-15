#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include <vector>

#include <parser/ParseTree.h>
#include <resolver/Plan.h>

namespace hustle {
namespace resolver {

enum PLAN_TYPE {
  SELECT,
  JOIN,
  UNKNOWN
};

class Resolver {
 public:
  std::shared_ptr<Plan> resolve(const std::shared_ptr<hustle::parser::ParseTree> &parse_tree) {
    std::vector<std::shared_ptr<QueryOperator>> query_operators(parse_tree->loop_pred.size());
    for (int i = 0; i < query_operators.size(); i++) {
      auto table = std::make_shared<TableReference>(i);
      auto select = std::make_shared<Select>();
      select->input = std::move(table);
      query_operators[i] = std::move(select);
    }

    for (auto &pred : parse_tree->other_pred) {
      if (pred->type == "ComparativeExpr") {
        auto expr = std::dynamic_pointer_cast<hustle::parser::ComparativeExpr>(pred);
        std::shared_ptr<Comparative> e = resolve_ComparativeExpr(expr);
        std::dynamic_pointer_cast<Select>(query_operators[e->left->i_table])->and_filter.push_back(e);
      } else if (pred->type == "DisjunctiveExpr") {
        auto expr = std::dynamic_pointer_cast<hustle::parser::DisjunctiveExpr>(pred);
        std::vector<std::shared_ptr<Comparative>> curr_exprs = resolve_DisjunctiveExpr(expr);
        for (std::shared_ptr<Comparative> &e : curr_exprs) {
          std::dynamic_pointer_cast<Select>(query_operators[e->left->i_table])->or_filter.push_back(e);
        }
      }
    }

    return nullptr;
  }

  std::shared_ptr<Expr> resolve_expr(
      const std::shared_ptr<hustle::parser::Expr> &expr) {
    if (expr->type == "Column") {
      return resolve_Column(std::dynamic_pointer_cast<hustle::parser::Column>(expr));
    } else if (expr->type == "IntLiteral") {
      return resolve_IntLiteral(std::dynamic_pointer_cast<hustle::parser::IntLiteral>(expr));
    } else if (expr->type == "IntLiteral") {
      return resolve_StrLiteral(std::dynamic_pointer_cast<hustle::parser::StrLiteral>(expr));
    } else if (expr->type == "Comparative") {
      return resolve_ComparativeExpr(std::dynamic_pointer_cast<hustle::parser::ComparativeExpr>(expr));
    } else {
      return nullptr;
    }
  }

  std::shared_ptr<Comparative> resolve_ComparativeExpr(const std::shared_ptr<hustle::parser::ComparativeExpr> &expr) {
    return std::make_shared<Comparative>(
        resolve_Column(std::dynamic_pointer_cast<hustle::parser::Column>(expr->left)),
        expr->op,
        resolve_expr(expr->right));
  }

  std::shared_ptr<ColumnReference> resolve_Column(const std::shared_ptr<hustle::parser::Column> &expr) {
    return std::make_shared<ColumnReference>(expr->column_name, expr->i_table, expr->i_column);
  }

  std::shared_ptr<IntLiteral> resolve_IntLiteral(const std::shared_ptr<hustle::parser::IntLiteral> &expr) {
    return std::make_shared<IntLiteral>(expr->value);
  }

  std::shared_ptr<StrLiteral> resolve_StrLiteral(const std::shared_ptr<hustle::parser::StrLiteral> &expr) {
    return std::make_shared<StrLiteral>(expr->value);
  }

  std::vector<std::shared_ptr<Comparative>> resolve_DisjunctiveExpr(const std::shared_ptr<hustle::parser::DisjunctiveExpr> &expr) {
    return std::vector<std::shared_ptr<Comparative>>{
        std::move(std::dynamic_pointer_cast<Comparative>(expr->left)),
        std::move(std::dynamic_pointer_cast<Comparative>(expr->right))
    };
  }

};

}
}

#endif //HUSTLE_RESOLVER_H
