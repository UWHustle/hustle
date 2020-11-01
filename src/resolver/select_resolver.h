
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

#ifndef HUSTLE_SELECT_RESOLVER_H
#define HUSTLE_SELECT_RESOLVER_H

#include <memory>
#include <string>
#include <unordered_map>

#include "operators/predicate.h"
#include "resolver/cresolver.h"

namespace hustle {
namespace resolver {
class SelectResolver {
 private:
  std::unordered_map<std::string,
                     std::shared_ptr<hustle::operators::PredicateTree>>
      select_predicates_;
  std::unordered_map<std::string,
                     std::shared_ptr<hustle::operators::PredicateTree>>
      join_predicates_;

  void ResolvePredExpr(Expr* pExpr);

 public:
  bool ResolveSelectTree(Select* queryTree);
};
}  // namespace resolver
}  // namespace hustle

#endif  // HUSTLE_SELECT_RESOLVER_H