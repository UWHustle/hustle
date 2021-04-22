// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "resolver/select_resolver.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <memory>
#include <set>

namespace hustle {
namespace resolver {

using namespace hustle::operators;
using namespace hustle::catalog;

bool SelectResolver::CheckJoinSupport() {
  for (auto const& [ltable_name, predicate_list] : join_graph_) {
    if (predicate_list.size() == predicates_.size()) {
      join_type_ = JoinType::STAR;
      return true;
    }
  }

  JoinPredicate start_predicate;
  std::string start_table_name;
  for (auto const& [ltable_name, predicate_list] : join_graph_) {
    if (predicate_list.size() == 1) {
      start_predicate = predicate_list.at(0);
      start_table_name = ltable_name;
    }
  }
  if (start_table_name.empty()) {
    return false;
  }

  int covered_preds = 0;
  JoinPredicate cur_predicate = start_predicate;
  std::string cur_table_name = start_table_name;

  std::set<std::string> visited_tables;
  while (covered_preds < predicates_.size()) {
    if (visited_tables.find(cur_table_name) != visited_tables.end())
      return false;
    visited_tables.insert(cur_table_name);
    std::string next_table =
        !cur_predicate.left_col_.table->get_name().compare(cur_table_name)
            ? cur_predicate.right_col_.table->get_name()
            : cur_predicate.left_col_.table->get_name();
    auto rpreds = join_graph_[next_table];
    if (rpreds.size() > 2) {
      join_type_ = JoinType::OTHER;
      return false;
    }
    covered_preds++;
    bool found_next_pred = false;
    for (auto pred : rpreds) {
      if (pred.left_col_.table != cur_predicate.left_col_.table ||
          pred.left_col_.col_name.compare(cur_predicate.left_col_.col_name) ||
          pred.right_col_.table != cur_predicate.right_col_.table ||
          pred.right_col_.col_name.compare(cur_predicate.right_col_.col_name)) {
        cur_predicate = pred;
        cur_table_name = next_table;
        found_next_pred = true;
      }
    }
    if (!found_next_pred && covered_preds < predicates_.size()) {
      return false;
    }
  }
  join_type_ = JoinType::LINEAR;
  return true;
}

void SelectResolver::ResolveJoinPredExpr(Expr* pExpr) {
  if (pExpr == NULL) {
    return;
  }

  switch (pExpr->op) {
    case TK_STRING:
    case TK_INTEGER:
    case TK_COLUMN: {
      break;
    }
    case TK_EQ: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;
      ColumnReference lRef, rRef;

      if ((leftExpr != NULL && leftExpr->op == TK_COLUMN) &&
          (rightExpr != NULL && rightExpr->op == TK_COLUMN)) {
        auto table_1 = catalog_->GetTable(leftExpr->y.pTab->zName);
        auto table_2 = catalog_->GetTable(rightExpr->y.pTab->zName);
        ColumnReference lRef = {
            table_1, leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        ColumnReference rRef = {
            table_2, rightExpr->y.pTab->aCol[rightExpr->iColumn].zName};

        if ((table_1 != nullptr && table_2 != nullptr) &&
            (table_1->get_num_rows() < table_2->get_num_rows())) {
          std::swap(lRef, rRef);
        }

        JoinPredicate join_pred = {lRef, arrow::compute::EQUAL, rRef};
        if (rRef.table != nullptr) {
          join_predicates_[rRef.table->get_name()] = join_pred;
          join_graph_[rRef.table->get_name()].emplace_back(join_pred);
          predicates_.emplace_back(join_pred);
        }

        if (lRef.table != nullptr) {
          join_graph_[lRef.table->get_name()].emplace_back(join_pred);
        }
      }
      break;
    }
    default: {
      ResolveJoinPredExpr(pExpr->pLeft);
      ResolveJoinPredExpr(pExpr->pRight);
    }
  }
}

std::shared_ptr<ExprReference> SelectResolver::ResolveAggExpr(Expr* expr) {
  if (expr == NULL) {
    return nullptr;
  }
  std::shared_ptr<ExprReference> expr_ref = std::make_shared<ExprReference>();
  switch (expr->op) {
    case TK_STAR:
    case TK_PLUS:
    case TK_MINUS:
    case TK_SLASH:
    case TK_COLUMN:
    case TK_AGG_COLUMN: {
      expr_ref->op = expr->op;
      if (expr->op == TK_COLUMN || expr->op == TK_AGG_COLUMN) {
        expr_ref->column_ref = std::make_shared<ColumnReference>();
        expr_ref->column_ref->table = catalog_->GetTable(expr->y.pTab->zName);
        expr_ref->column_ref->col_name =
            expr->y.pTab->aCol[expr->iColumn].zName;
      }
      expr_ref->left_expr = this->ResolveAggExpr(expr->pLeft);
      expr_ref->right_expr = this->ResolveAggExpr(expr->pRight);
      break;
    }
    default: {
      // Contains operator that is not supported
      return nullptr;
    }
  }
  return expr_ref;
}

std::shared_ptr<PredicateTree> SelectResolver::ResolvePredExpr(Expr* pExpr) {
  if (pExpr == NULL) {
    return nullptr;
  }
  arrow::compute::CompareOperator comparatorOperator;
  std::shared_ptr<PredicateTree> predicate_tree = nullptr;
  switch (pExpr->op) {
    case TK_INTEGER:
    case TK_STRING:
    case TK_OR:
    case TK_AND: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;

      std::shared_ptr<PredicateTree> lpred_tree = ResolvePredExpr(leftExpr);
      std::shared_ptr<PredicateTree> rpred_tree = ResolvePredExpr(rightExpr);

      if (lpred_tree == nullptr && rpred_tree == nullptr) break;
      if (lpred_tree == nullptr || rpred_tree == nullptr) {
        auto cur_pred_tree = (lpred_tree == nullptr) ? rpred_tree : lpred_tree;
        if (pExpr->op == TK_AND) {
          auto prev_pred_tree = select_predicates_[cur_pred_tree->table_name_];
          if (prev_pred_tree != cur_pred_tree) {
            std::shared_ptr<ConnectiveNode> connective_node =
                std::make_shared<ConnectiveNode>(prev_pred_tree.get()->root_,
                                                 cur_pred_tree.get()->root_,
                                                 FilterOperator::AND);
            predicate_tree = std::make_shared<PredicateTree>(connective_node);
            select_predicates_[cur_pred_tree->table_name_] = predicate_tree;
          }
        }
        break;
      }
      if (leftExpr->pLeft->iTable == rightExpr->pLeft->iTable) {
        if (pExpr->op == TK_OR) {
          std::shared_ptr<ConnectiveNode> connective_node =
              std::make_shared<ConnectiveNode>(lpred_tree.get()->root_,
                                               rpred_tree.get()->root_,
                                               FilterOperator::OR);
          predicate_tree = std::make_shared<PredicateTree>(connective_node);
        }
        if (pExpr->op == TK_AND) {
          std::shared_ptr<ConnectiveNode> connective_node =
              std::make_shared<ConnectiveNode>(lpred_tree.get()->root_,
                                               rpred_tree.get()->root_,
                                               FilterOperator::AND);
          predicate_tree = std::make_shared<PredicateTree>(connective_node);
        }
        predicate_tree->table_id_ = lpred_tree->table_id_;
        predicate_tree->table_name_ = lpred_tree->table_name_;
        select_predicates_[predicate_tree->table_name_] = predicate_tree;
      } else {
        if (pExpr->op == TK_OR) {
          throw std::runtime_error(
              "Logical OR operator on different table is not supported.");
        }
      }
      break;
    }
    case TK_NE:
    case TK_EQ:
    case TK_GT:
    case TK_LE:
    case TK_LT:
    case TK_GE: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;

      ColumnReference colRef;
      arrow::Datum datum;
      if ((leftExpr != NULL && leftExpr->op == TK_COLUMN) &&
          (rightExpr != NULL &&
           (rightExpr->op == TK_INTEGER || rightExpr->op == TK_STRING))) {
        colRef = {catalog_->GetTable(leftExpr->y.pTab->zName),
                  leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        if (rightExpr->op == TK_STRING) {
          datum = arrow::Datum(
              std::make_shared<arrow::StringScalar>(rightExpr->u.zToken));
        } else if (rightExpr->op == TK_INTEGER) {
          datum = arrow::Datum((int64_t)rightExpr->u.iValue);
        }
        if (pExpr->op == TK_NE)
          comparatorOperator = arrow::compute::CompareOperator::NOT_EQUAL;
        if (pExpr->op == TK_EQ)
          comparatorOperator = arrow::compute::CompareOperator::EQUAL;
        if (pExpr->op == TK_LT)
          comparatorOperator = arrow::compute::CompareOperator::LESS;
        if (pExpr->op == TK_LE)
          comparatorOperator = arrow::compute::CompareOperator::LESS_EQUAL;
        if (pExpr->op == TK_GT)
          comparatorOperator = arrow::compute::CompareOperator::GREATER;
        if (pExpr->op == TK_GE)
          comparatorOperator = arrow::compute::CompareOperator::GREATER_EQUAL;
        Predicate predicate = {colRef, comparatorOperator, datum};
        auto predicate_node = std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(predicate));
        predicate_tree = std::make_shared<PredicateTree>(predicate_node);
        predicate_tree->table_id_ = leftExpr->iTable;
        predicate_tree->table_name_ = std::string(leftExpr->y.pTab->zName);
        if (select_predicates_[leftExpr->y.pTab->zName] == nullptr) {
          select_predicates_[leftExpr->y.pTab->zName] = predicate_tree;
        }
      } else {
        if (!((leftExpr != NULL && leftExpr->op == TK_COLUMN) &&
              (rightExpr != NULL && rightExpr->op == TK_COLUMN))) {
          resolve_status_ = false;
        }
        return nullptr;
      }
      break;
    }
    case TK_BETWEEN: {
      ColumnReference colRef;
      arrow::Datum ldatum, rdatum;
      Expr* leftExpr = pExpr->pLeft;
      if (leftExpr != NULL && leftExpr->op == TK_COLUMN) {
        colRef = {catalog_->GetTable(leftExpr->y.pTab->zName),
                  leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        Expr* firstExpr = pExpr->x.pList->a[0].pExpr;
        Expr* secondExpr = pExpr->x.pList->a[1].pExpr;
        if (firstExpr->op == TK_INTEGER && secondExpr->op == TK_INTEGER) {
          ldatum = arrow::Datum((int64_t)firstExpr->u.iValue);
          rdatum = arrow::Datum((int64_t)secondExpr->u.iValue);
          Predicate left_predicate = {
              colRef, arrow::compute::CompareOperator::GREATER_EQUAL, ldatum};
          Predicate right_predicate = {
              colRef, arrow::compute::CompareOperator::LESS_EQUAL, rdatum};
          auto left_predicate_node = std::make_shared<PredicateNode>(
              std::make_shared<Predicate>(left_predicate));
          auto right_predicate_node = std::make_shared<PredicateNode>(
              std::make_shared<Predicate>(right_predicate));
          std::shared_ptr<ConnectiveNode> connective_node =
              std::make_shared<ConnectiveNode>(left_predicate_node,
                                               right_predicate_node,
                                               FilterOperator::AND);
          predicate_tree = std::make_shared<PredicateTree>(connective_node);
          predicate_tree->table_id_ = leftExpr->iTable;
          predicate_tree->table_name_ = std::string(leftExpr->y.pTab->zName);
          select_predicates_[leftExpr->y.pTab->zName] = predicate_tree;
        } else {
          return nullptr;
        }
      }
      break;
    }
    case TK_AGG_COLUMN:
    case TK_AGG_FUNCTION:
    case TK_COLUMN: {
      break;
    }
    default: {
      resolve_status_ = false;
    }
  }
  // predicate_tree is NULL it it contains operator not supported
  return predicate_tree;
}

bool SelectResolver::ResolveSelectTree(Sqlite3Select* queryTree) {
  // Collect all the src tables
  SrcList* pTabList = queryTree->pSrc;
  if (pTabList == NULL) {
    return false;
  }

  // Currently having construct is not handled in Hustle
  if (queryTree->pHaving != NULL) {
    return false;
  }

  // Unsupported types of select queries in Hustle
  if ((queryTree->selFlags & SF_Distinct) || (queryTree->selFlags & SF_All) ||
      (queryTree->selFlags & SF_NestedFrom) ||
      (queryTree->selFlags & SF_View) || (queryTree->selFlags & SF_Recursive) ||
      (queryTree->selFlags & SF_FixedLimit) ||
      (queryTree->selFlags & SF_Compound) ||
      (queryTree->selFlags & SF_Values) ||
      (queryTree->selFlags & SF_MultiValue)) {
    return false;
  }

  for (int i = 0; i < pTabList->nSrc; i++) {
    // Select as table source is not supported
    if (pTabList->a[i].pSelect != NULL) {
      return false;
    }
    if (pTabList->a[i].pUsing) return false;
    if (pTabList->a[i].pOn) {
      return false;
    }
    if (pTabList->a[i].fg.jointype & JT_UNSUPPORTED) {
      return false;
    }

    select_predicates_.insert({pTabList->a[i].zName, nullptr});
  }

  ExprList* pEList = queryTree->pEList;
  for (int k = 0; k < pEList->nExpr; k++) {
    ResolvePredExpr(pEList->a[k].pExpr);
  }

  for (int k = 0; k < pEList->nExpr; k++) {
    if (pEList->a[k].pExpr->op == TK_AGG_FUNCTION) {
      Expr* expr = pEList->a[k].pExpr->x.pList->a[0].pExpr;
      char* zName = NULL;
      if (pEList->a[k].zEName != NULL) {
        zName = pEList->a[k].zEName;
      }

      std::string agg_func_name(pEList->a[k].pExpr->u.zToken);
      std::transform(agg_func_name.begin(), agg_func_name.end(),
                     agg_func_name.begin(), ::toupper);
      if (expr->iColumn > 0) {
        ColumnReference colRef = {catalog_->GetTable(expr->y.pTab->zName),
                                  expr->y.pTab->aCol[expr->iColumn].zName};
        if (aggregate_kernels_.find(agg_func_name) ==
            aggregate_kernels_.end()) {
          return false;
        }
        AggregateReference aggRef = {aggregate_kernels_[agg_func_name],
                                     pEList->a[k].zEName, colRef};
        agg_references_->emplace_back(aggRef);
        std::shared_ptr<ProjectReference> projRef =
            std::make_shared<ProjectReference>(ProjectReference{colRef, zName});
        project_references_->emplace_back(projRef);
      } else {
        std::shared_ptr<ExprReference> expr_ref = ResolveAggExpr(expr);
        if (expr_ref == nullptr) {
          return false;
        }
        if (aggregate_kernels_.find(agg_func_name) ==
            aggregate_kernels_.end()) {
          return false;
        }
        ColumnReference colRef = {};
        AggregateReference aggRef = {aggregate_kernels_[agg_func_name],
                                     pEList->a[k].zEName, colRef, expr_ref};
        agg_references_->emplace_back(aggRef);
        std::shared_ptr<ProjectReference> projRef =
            std::make_shared<ProjectReference>(ProjectReference{colRef, zName});
        project_references_->emplace_back(projRef);
      }
    } else if (pEList->a[k].pExpr->op == TK_COLUMN ||
               pEList->a[k].pExpr->op == TK_AGG_COLUMN) {
      Expr* expr = pEList->a[k].pExpr;
      if (expr->iColumn == -1) {  // For ROWID case
        return false;
      }
      ColumnReference colRef = {catalog_->GetTable(expr->y.pTab->zName),
                                expr->y.pTab->aCol[expr->iColumn].zName};
      std::shared_ptr<ProjectReference> projRef =
          std::make_shared<ProjectReference>(ProjectReference{colRef});
      project_references_->emplace_back(projRef);
    } else {
      // Other than AGG FUNCTION or COLUMN
      // it is unsupported
      return false;
    }
  }

  if (queryTree->pNext) return false;

  // Resolve predicates
  Expr* pWhere = queryTree->pWhere;
  if (pWhere != NULL) {
    ResolvePredExpr(pWhere);
    if (!resolve_status_) {
      return false;
    }
  }
  if (pWhere != NULL) {
    ResolveJoinPredExpr(pWhere);
  }

  // Resolve GroupBy
  ExprList* pGroupBy = queryTree->pGroupBy;
  if (pGroupBy != NULL) {
    for (int i = 0; i < pGroupBy->nExpr; i++) {
      if (pGroupBy->a[i].pExpr->iColumn >= 0) {
        std::shared_ptr<ColumnReference> colRef =
            std::make_shared<ColumnReference>(ColumnReference{
                catalog_->GetTable(pGroupBy->a[i].pExpr->y.pTab->zName),
                pGroupBy->a[i]
                    .pExpr->y.pTab->aCol[pGroupBy->a[i].pExpr->iColumn]
                    .zName});
        group_by_references_->emplace_back(colRef);
      }
    }
  }

  // Resolve OrderBy
  ExprList* pOrderBy = queryTree->pOrderBy;
  if (pOrderBy != NULL) {
    for (int i = 0; i < pOrderBy->nExpr; i++) {
      if (pOrderBy->a[i].u.x.iOrderByCol > 0) {
        if (pOrderBy->a[i].pExpr->iColumn >= 0) {
          std::shared_ptr<OrderByReference> order_ref;
          if (pOrderBy->a[i].pExpr->op == TK_AGG_FUNCTION) {
            order_ref = std::make_shared<OrderByReference>(OrderByReference{
                nullptr,
                (*project_references_)[pOrderBy->a[i].u.x.iOrderByCol - 1]
                    ->alias,
                (bool)(pOrderBy->a[i].sortFlags & KEYINFO_ORDER_DESC)});
          } else {
            order_ref = std::make_shared<OrderByReference>(OrderByReference{
                catalog_->GetTable(pOrderBy->a[i].pExpr->y.pTab->zName),
                pOrderBy->a[i]
                    .pExpr->y.pTab->aCol[pOrderBy->a[i].pExpr->iColumn]
                    .zName,
                (bool)(pOrderBy->a[i].sortFlags & KEYINFO_ORDER_DESC)});
          }
          order_by_references_->emplace_back(order_ref);
        }
      }
    }
  }
  return true;  // TODO: return true or false based on query resolvability
}
}  // namespace resolver
}  // namespace hustle
