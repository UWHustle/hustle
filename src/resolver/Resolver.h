#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include <vector>
#include <cassert>

#include <parser/ParseTree.h>
#include <resolver/Plan.h>

namespace hustle {
namespace resolver {

class Resolver {
 public:
  /**
   * This function takes as input a parse tree from the parser, builds the plan
   * from bottom to top given the predicates and expressions preserved in the
   * parse tree.
   *
   * First of all, a number of TableReference objects are built. On top of each
   * TableReference, a Select object is built to contain it. By traversing
   * the parse_tree->other_pred, we match each select predicate as a filter
   * onto the corresponding Select object. This step is to push all the select
   * predicates down to the TableReference objects as much as possible. The
   * Join objects are then built on top of these Select objects by traversing
   * parse_tree->loop_pred. The topmost Join object is then captured as an
   * input to the GroupBy, Project, and OrderBy.
   *
   * The resolving order is: TableReference, Select, Join, GroupBy, Project
   * and OrderBy.
   *
   * @param parse_tree: input parse tree from parser
   */
  void resolve(const std::shared_ptr<hustle::parser::ParseTree> &parse_tree) {
    std::vector<std::shared_ptr<Select>>
        select_operators(parse_tree->loop_pred.size());
    for (int i = 0; i < select_operators.size(); i++) {
      select_operators[i] = std::make_shared<Select>(
          std::make_shared<TableReference>(i));
    }

    // resolve Select
    for (auto &pred : parse_tree->other_pred) {
      if (pred->type == +ExprType::Comparative) {
        auto e = resolve_Comparative(
            std::dynamic_pointer_cast<hustle::parser::Comparative>(pred));
        select_operators[e->left->i_table]->filter.push_back(e);
      } else { // pred->type == +ExprType::Disjunctive
        assert(pred->type == +ExprType::Disjunctive);
        auto e = resolve_Disjunctive(
            std::dynamic_pointer_cast<hustle::parser::Disjunctive>(pred));
        select_operators[e->i_table]->filter.push_back(e);
      }
    }

    // resolve Join
    std::shared_ptr<QueryOperator> root;
    for (auto &lpred : parse_tree->loop_pred) {
      if (root == nullptr) {
        root = std::move(select_operators[lpred->fromtable]);
      } else {
        std::vector<std::shared_ptr<Comparative>> join_preds;
        for (auto &pred : lpred->predicates) {
          join_preds.push_back(resolve_Comparative(pred));
        }
        auto right = std::move(select_operators[lpred->fromtable]);
        root = std::make_shared<Join>(
            std::move(root),
            std::move(right),
            std::move(join_preds)
        );
      }
    }

    // resolve GroupBy
    if (!parse_tree->group_by.empty()) {
      std::vector<std::shared_ptr<ColumnReference>> groupby_cols;
      for (auto &col : parse_tree->group_by) {
        groupby_cols.push_back(resolve_ColumnReference(col));
      }
      root = std::make_shared<GroupBy>(
          std::move(root),
          std::move(groupby_cols));
    }

    // resolve Project
    {
      std::vector<std::shared_ptr<Expr>> proj_exprs;
      std::vector<std::string> proj_names;

      for (auto &proj : parse_tree->project) {
        proj_exprs.push_back(resolve_Expr(proj->expr));
        proj_names.push_back(proj->proj_name);
      }
      root = std::make_shared<Project>(std::move(root),
                                       std::move(proj_exprs),
                                       std::move(proj_names));
    }

    // resolve OrderBy
    if (!parse_tree->order_by.empty()) {
      std::vector<std::shared_ptr<Expr>> orderby_cols;
      std::vector<OrderByDirection> orders;

      for (auto &orderby : parse_tree->order_by) {
        orderby_cols.push_back(resolve_Expr(orderby->expr));
        orders.push_back(orderby->order);
      }
      root = std::make_shared<OrderBy>(std::move(root),
                                       std::move(orderby_cols),
                                       std::move(orders));
    }

    plan_ = std::make_shared<Query>(root);
  }

  /**
   * Function to resolve hustle::parser::Expr
   */
  std::shared_ptr<Expr> resolve_Expr(
      const std::shared_ptr<hustle::parser::Expr> &expr) {
    switch (expr->type) {
      case ExprType::ColumnReference:
        return resolve_ColumnReference(
            std::dynamic_pointer_cast<hustle::parser::ColumnReference>(expr));

      case ExprType::IntLiteral:
        return resolve_IntLiteral(
            std::dynamic_pointer_cast<hustle::parser::IntLiteral>(expr));

      case ExprType::StrLiteral:
        return resolve_StrLiteral(
            std::dynamic_pointer_cast<hustle::parser::StrLiteral>(expr));

      case ExprType::Comparative:
        return resolve_Comparative(
            std::dynamic_pointer_cast<hustle::parser::Comparative>(expr));

      case ExprType::Disjunctive:
        return resolve_Disjunctive(
            std::dynamic_pointer_cast<hustle::parser::Disjunctive>(expr));

      case ExprType::Arithmetic:
        return resolve_Arithmetic(
            std::dynamic_pointer_cast<hustle::parser::Arithmetic>(expr));

      case ExprType::AggFunc:
        return resolve_AggFunc(
            std::dynamic_pointer_cast<hustle::parser::AggFunc>(expr));

      default:
        return nullptr;
    }
  }

  /**
   * Function to resolve hustle::parser::Comparative
   */
  std::shared_ptr<Comparative> resolve_Comparative(
      const std::shared_ptr<hustle::parser::Comparative> &expr) {
    return std::make_shared<Comparative>(resolve_ColumnReference(
        std::dynamic_pointer_cast<hustle::parser::ColumnReference>(expr->left)),
                                         expr->op,
                                         resolve_Expr(expr->right));
  }

  /**
   * Function to resolve hustle::parser::ColumnReference
   */
  std::shared_ptr<ColumnReference> resolve_ColumnReference(
      const std::shared_ptr<hustle::parser::ColumnReference> &expr) {
    return std::make_shared<ColumnReference>(expr->column_name,
                                             expr->i_table,
                                             expr->i_column);
  }

  /**
   * Function to resolve hustle::parser::IntLiteral
   */
  std::shared_ptr<IntLiteral> resolve_IntLiteral(
      const std::shared_ptr<hustle::parser::IntLiteral> &expr) {
    return std::make_shared<IntLiteral>(expr->value);
  }

  /**
   * Function to resolve hustle::parser::StrLiteral
   */
  std::shared_ptr<StrLiteral> resolve_StrLiteral(
      const std::shared_ptr<hustle::parser::StrLiteral> &expr) {
    return std::make_shared<StrLiteral>(expr->value);
  }

  /**
   * Function to resolve hustle::parser::Disjunctive
   */
  std::shared_ptr<Disjunctive> resolve_Disjunctive(
      const std::shared_ptr<hustle::parser::Disjunctive> &expr) {
    auto left = resolve_Comparative(
        std::dynamic_pointer_cast<hustle::parser::Comparative>(expr->left));
    auto right = resolve_Comparative(
        std::dynamic_pointer_cast<hustle::parser::Comparative>(expr->right));
    int i_table = left->left->i_table;

    return std::make_shared<Disjunctive>(
        i_table,
        std::vector<std::shared_ptr<Comparative>>{
            std::move(left),
            std::move(right)
        });
  }

  /**
   * Function to resolve hustle::parser::Arithmetic
   */
  std::shared_ptr<Arithmetic> resolve_Arithmetic(
      const std::shared_ptr<hustle::parser::Arithmetic> &expr) {
    return std::make_shared<Arithmetic>(resolve_Expr(expr->left),
                                        expr->op,
                                        resolve_Expr(expr->right));
  }

  /**
   * Function to resolve hustle::parser::AggFunc
   */
  std::shared_ptr<AggFunc> resolve_AggFunc(
      const std::shared_ptr<hustle::parser::AggFunc> &expr) {
    return std::make_shared<AggFunc>(expr->func,
                                     resolve_Expr(expr->expr));
  }

  /**
   * Function to serialize the parse tree
   */
  std::string to_string(int indent) {
    json j = plan_;
    return j.dump(indent);
  }

 private:
  std::shared_ptr<Plan> plan_;
};

}
}

#endif //HUSTLE_RESOLVER_H
