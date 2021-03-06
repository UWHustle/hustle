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
#include <cstring>
#include <algorithm>
#include <memory>
#include <cctype>

namespace hustle {
namespace resolver {

using namespace hustle::operators;
using namespace hustle::catalog;

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
        auto table_1 = catalog_->getTable(leftExpr->y.pTab->zName);
        auto table_2 = catalog_->getTable(rightExpr->y.pTab->zName);
        ColumnReference lRef = {
            table_1, leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        ColumnReference rRef = {
            table_2, rightExpr->y.pTab->aCol[rightExpr->iColumn].zName};

        if ((table_1 != nullptr && table_1->get_num_rows()) <
            (table_2 != nullptr && table_2->get_num_rows())) {
          std::swap(lRef, rRef);
        }

        JoinPredicate join_pred = {lRef, arrow::compute::EQUAL, rRef};
        if (rRef.table != nullptr) {
          join_predicates_[rRef.table->get_name()] = join_pred;
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
        expr_ref->column_ref->table = catalog_->getTable(expr->y.pTab->zName);
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
  FilterOperator connective;
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

      if (lpred_tree == nullptr || rpred_tree == nullptr) break;
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
        colRef = {catalog_->getTable(leftExpr->y.pTab->zName),
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
        select_predicates_[leftExpr->y.pTab->zName] = predicate_tree;
      }
      break;
    }
    case TK_BETWEEN: {
        ColumnReference colRef;
        arrow::Datum ldatum, rdatum;
        Expr* leftExpr = pExpr->pLeft;
        if (leftExpr != NULL && leftExpr->op == TK_COLUMN){
            colRef = {catalog_->getTable(leftExpr->y.pTab->zName),
                      leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
            Expr* firstExpr = pExpr->x.pList->a[0].pExpr;
            Expr* secondExpr = pExpr->x.pList->a[1].pExpr;
            if (firstExpr->op == TK_INTEGER && secondExpr->op == TK_INTEGER) {
                ldatum = arrow::Datum((int64_t)firstExpr->u.iValue);
                rdatum = arrow::Datum((int64_t)secondExpr->u.iValue);
                Predicate left_predicate = {colRef, arrow::compute::CompareOperator::GREATER_EQUAL, ldatum};
                Predicate right_predicate = {colRef, arrow::compute::CompareOperator::LESS_EQUAL, rdatum};
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
  }
  return predicate_tree;
}

bool SelectResolver::ResolveSelectTree(Sqlite3Select* queryTree) {
  // Collect all the src tables
  SrcList* pTabList = queryTree->pSrc;
  if (pTabList == NULL) {
    return false;
  }
  for (int i = 0; i < pTabList->nSrc; i++) {
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
      std::transform(agg_func_name.begin(), agg_func_name.end(), agg_func_name.begin(), ::toupper);
      if (expr->iColumn > 0) {
        ColumnReference colRef = {catalog_->getTable(expr->y.pTab->zName),
                                  expr->y.pTab->aCol[expr->iColumn].zName};
        AggregateReference aggRef = {
               aggregate_kernels_[agg_func_name],
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
        ColumnReference colRef = {};
        AggregateReference aggRef = {
            aggregate_kernels_[agg_func_name],
            pEList->a[k].zEName, colRef, expr_ref};
        agg_references_->emplace_back(aggRef);
        std::shared_ptr<ProjectReference> projRef =
            std::make_shared<ProjectReference>(ProjectReference{colRef, zName});
        project_references_->emplace_back(projRef);
      }
    } else if (pEList->a[k].pExpr->op == TK_COLUMN ||
               pEList->a[k].pExpr->op == TK_AGG_COLUMN) {
      Expr* expr = pEList->a[k].pExpr;
      ColumnReference colRef = {catalog_->getTable(expr->y.pTab->zName),
                                expr->y.pTab->aCol[expr->iColumn].zName};
      std::shared_ptr<ProjectReference> projRef =
          std::make_shared<ProjectReference>(ProjectReference{colRef});
      project_references_->emplace_back(projRef);
    }
  }

  // Resolve predicates
  Expr* pWhere = queryTree->pWhere;
  if (pWhere != NULL) {
    ResolvePredExpr(pWhere);
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
                catalog_->getTable(pGroupBy->a[i].pExpr->y.pTab->zName),
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
          std::shared_ptr<ColumnReference> colRef;
          if (pOrderBy->a[i].pExpr->op == TK_AGG_FUNCTION) {
            colRef = std::make_shared<ColumnReference>(ColumnReference{
                nullptr,
                (*project_references_)[pOrderBy->a[i].u.x.iOrderByCol - 1]
                    ->alias});
          } else {
            colRef = std::make_shared<ColumnReference>(ColumnReference{
                catalog_->getTable(pOrderBy->a[i].pExpr->y.pTab->zName),
                pOrderBy->a[i]
                    .pExpr->y.pTab->aCol[pOrderBy->a[i].pExpr->iColumn]
                    .zName});
          }
          order_by_references_->emplace_back(colRef);
        }
      }
    }
  }
  return true;  // TODO: return true or false based on query resolvability
}
}  // namespace resolver
}  // namespace hustle
