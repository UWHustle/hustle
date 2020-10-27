#include "resolver/cresolver.h"

#include <iostream>

int resolveSelect(Select* queryTree) {
  // TODO: (@srsuryadev) resolve the select query
  if (queryTree->pEList != NULL) {
    std::cout << queryTree->pEList->a->zEName << std::endl;
  }
  if (queryTree->pGroupBy != NULL && queryTree->pEList != NULL) {
    std::cout << queryTree->pGroupBy->a->zEName << std::endl;
  }
  return 0;
}