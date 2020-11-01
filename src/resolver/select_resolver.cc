
#include "resolver/select_resolver.h"

namespace hustle {
namespace resolver {

void SelectResolver::ResolvePredExpr(Expr* pExpr) {
  switch (pExpr->op) {
    case TK_INTEGER: {
      break;
    }
    case TK_STRING: {
      break;
    }
    case TK_COLUMN: {
      if (pExpr->iColumn < 0) return;
      std::cout << "Column Index: " << pExpr->iColumn << std::endl;
      std::cout << "Column Name: " << pExpr->y.pTab->aCol[pExpr->iColumn].zName
                << std::endl;
      break;
    }
    case TK_OR: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_AND: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_NE: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_EQ: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_GT: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_LE: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_LT: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
    case TK_GE: {
      ResolvePredExpr(pExpr->pLeft);
      ResolvePredExpr(pExpr->pRight);
      break;
    }
  }
}

bool SelectResolver::ResolveSelectTree(Select* queryTree) {
  // Collect all the src tables
  SrcList* pTabList = queryTree->pSrc;
  for (int i = 0; i < pTabList->nSrc; i++) {
    std::cout << "Join tables: " << pTabList->a[i].zName << std::endl;
    select_predicates_[pTabList->a[i].zName] = nullptr;
  }

  Expr* pWhere = queryTree->pWhere;
  ResolvePredExpr(pWhere);
  return true;
}
}  // namespace resolver
}  // namespace hustle
