#include "resolver/cresolver.h"

#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "resolver/select_resolver.h"

int resolveSelect(char* dbName, Sqlite3Select* queryTree) {
  ExprList* pEList = queryTree->pEList;
  Expr* pWhere = queryTree->pWhere;
  ExprList* pGroupBy = queryTree->pGroupBy;
  Expr* pHaving = queryTree->pHaving;
  ExprList* pOrderBy = queryTree->pOrderBy;

  // TODO: (@srsuryadev) resolve the select query
  // return 0 if query is supported in column store else return 1
  using hustle::resolver::SelectResolver;
  Catalog* catalog = hustle::HustleDB::getCatalog(dbName).get();
  if (dbName == NULL || catalog == nullptr) return 0;

  SelectResolver* select_resolver = new SelectResolver(catalog);
  select_resolver->ResolveSelectTree(queryTree);
  delete select_resolver;
  return 0;
}
