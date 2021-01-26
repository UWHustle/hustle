
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

#include "catalog/catalog.h"
#include "operators/aggregate.h"
#include "operators/predicate.h"
#include "resolver/cresolver.h"

namespace hustle {
namespace resolver {

using namespace hustle::operators;
using namespace hustle::catalog;

struct ProjectReference {
  ColumnReference colRef;
  std::string alias;
};

class SelectResolver {
 private:
  std::unordered_map<std::string,
                     std::shared_ptr<hustle::operators::PredicateTree>>
      select_predicates_;

  std::unordered_map<std::string, JoinPredicate> join_predicates_;
  std::shared_ptr<std::vector<AggregateReference>> agg_references_;
  std::shared_ptr<std::vector<std::shared_ptr<ColumnReference>>>
      group_by_references_;
  std::shared_ptr<std::vector<std::shared_ptr<ColumnReference>>>
      order_by_references_;
  std::shared_ptr<std::vector<std::shared_ptr<ProjectReference>>>
      project_references_;

  std::map<std::string, hustle::operators::AggregateKernel> aggregate_kernels_ =
      {{"SUM", AggregateKernel::SUM},
       {"COUNT", AggregateKernel::COUNT},
       {"MEAN", AggregateKernel::MEAN}};

  Catalog* catalog_;

  bool resolve_status_;

  std::shared_ptr<PredicateTree> ResolvePredExpr(Expr* pExpr);
  void ResolveJoinPredExpr(Expr* pExpr);

 public:
  SelectResolver(Catalog* catalog) : catalog_(catalog) {
    agg_references_ = std::make_shared<std::vector<AggregateReference>>();
    group_by_references_ =
        std::make_shared<std::vector<std::shared_ptr<ColumnReference>>>();
    order_by_references_ =
        std::make_shared<std::vector<std::shared_ptr<ColumnReference>>>();
    project_references_ =
        std::make_shared<std::vector<std::shared_ptr<ProjectReference>>>();

    agg_references_->reserve(5);
    group_by_references_->reserve(5);
    order_by_references_->reserve(5);
    project_references_->reserve(5);

    resolve_status_ = true;
  }

  SelectResolver() : SelectResolver(nullptr) {}

  inline std::unordered_map<std::string,
                     std::shared_ptr<hustle::operators::PredicateTree>>&
  select_predicates() {
    return select_predicates_;
  }

  inline std::unordered_map<std::string, JoinPredicate> join_predicates() {
    return join_predicates_;
  }

  inline std::shared_ptr<std::vector<AggregateReference>> agg_references() {
    return agg_references_;
  }

  inline std::shared_ptr<std::vector<std::shared_ptr<ColumnReference>>>
  groupby_references() {
    return group_by_references_;
  }

  inline std::shared_ptr<std::vector<std::shared_ptr<ColumnReference>>>
  orderby_references() {
    return order_by_references_;
  }

  inline std::shared_ptr<std::vector<std::shared_ptr<ProjectReference>>>
  project_references() {
    return project_references_;
  }

  bool ResolveSelectTree(Sqlite3Select* queryTree);
  bool ResolveSelectTree() { return true; }
};
}  // namespace resolver
}  // namespace hustle

#endif  // HUSTLE_SELECT_RESOLVER_H
