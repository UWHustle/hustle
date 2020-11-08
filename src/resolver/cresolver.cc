#include "resolver/cresolver.h"

#include <iostream>

#include "resolver/select_resolver.h"

int resolveSelect(Sqlite3Select* queryTree) {
  ExprList* pEList = queryTree->pEList;
  Expr* pWhere = queryTree->pWhere;
  ExprList* pGroupBy = queryTree->pGroupBy;
  Expr* pHaving = queryTree->pHaving;
  ExprList* pOrderBy = queryTree->pOrderBy;

  // TODO: (@srsuryadev) resolve the select query
  // return 0 if query is supported in column store else return 1

  using hustle::resolver::SelectResolver;
  SelectResolver* select_resolver = new SelectResolver();
  select_resolver->ResolveSelectTree(queryTree);
  return 0;
}
