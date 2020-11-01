#include "resolver/cresolver.h"

#include <iostream>

void resolveExpr(Expr* pExpr) {
  /*
  switch (pExpr->op) {
    case TK_INTEGER: {
      currPos += sprintf(currPos, "{\"type\": \"%s\", \"value\": %d}",
                         "IntLiteral", pExpr->u.iValue);
      break;
    }
    case TK_STRING: {
      currPos += sprintf(currPos, "{\"type\": \"%s\", \"value\": \"%s\"}",
                         "StrLiteral", pExpr->u.zToken);
      break;
    }
    case TK_COLUMN: {
    }

    case TK_AGG_COLUMN: {
      if (pExpr->iColumn < 0) return;
      currPos +=
          sprintf(currPos,
                  "{\"type\": \"%s\", \"column_name\": \"%s\", \"i_table\": "
                  "%d, \"i_column\": %d}",
                  "ColumnReference", pExpr->y.pTab->aCol[pExpr->iColumn].zName,
                  pExpr->iTable, pExpr->iColumn);
      break;
    }
    case TK_OR: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Disjunctive");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"right\": ");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_AND: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Conjunctive");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"right\": ");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_NE: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "NE");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_EQ: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "EQ");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_GT: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "GT");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_LE: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "LE");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_LT: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "LT");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_GE: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Comparative");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "GE");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_PLUS: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Arithmetic");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "PLUS");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_MINUS: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Arithmetic");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "MINUS");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_STAR: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Arithmetic");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "STAR");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_SLASH: {
      currPos +=
          sprintf(currPos, "{\"type\": \"%s\", \"left\": ", "Arithmetic");
      resolveExpr(pExpr->pLeft);
      currPos += sprintf(currPos, ", \"op\": \"%s\", \"right\": ", "SLASH");
      resolveExpr(pExpr->pRight);
      currPos += sprintf(currPos, "}");
      break;
    }
    case TK_AGG_FUNCTION: {
      currPos += sprintf(
          currPos, "{\"type\": \"%s\", \"func\": \"%s\", \"expr\": ", "AggFunc",
          pExpr->pAggInfo->aFunc->pFunc->zName);
      resolveExpr(pExpr->x.pList->a[0].pExpr);
      currPos += sprintf(currPos, "}");
      break;
    }
  }*/
}

int resolveSelect(Select* queryTree) {
  ExprList* pEList = queryTree->pEList;
  Expr* pWhere = queryTree->pWhere;
  ExprList* pGroupBy = queryTree->pGroupBy;
  Expr* pHaving = queryTree->pHaving;
  ExprList* pOrderBy = queryTree->pOrderBy;

  // TODO: (@srsuryadev) resolve the select query
  // return 0 if query is supported in column store else return 1

  return 0;
}
