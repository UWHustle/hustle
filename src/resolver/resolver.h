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

#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include <cassert>
#include <vector>

#include "api/hustle_db.h"
#include "catalog/table_schema.h"
#include "parser/parse_tree.h"
#include "resolver/plan.h"

namespace hustle {
namespace resolver {

class Resolver {
 public:
  Resolver(hustle::catalog::Catalog *catalog) : catalog_(catalog) {}

  /**
   * This function takes as input a parse tree from the parser, builds the
   * plan from bottom to top given the predicates and expressions preserved
   * in the parse tree.
   *
   * First of all, a number of TableReference objects are built. On top of
   * each TableReference, a Select object is built to contain it. By
   * traversing the parse_tree->other_pred, we match each select predicate as
   * a filter onto the corresponding Select object. This step is to push all
   * the select predicates down to the TableReference objects as much as
   * possible. The Join objects are then built on top of these Select objects
   * by traversing parse_tree->loop_pred. The topmost Join object is then
   * captured as an input to the Aggregate, Project, and OrderBy.
   *
   * The resolving order is: TableReference, Select, Join, Aggregate, Project
   * and OrderBy.
   *
   * @param parse_tree: input parse tree from parser
   * @param hustleDB: input hustle database
   */

  void resolve(const std::shared_ptr<hustle::parser::ParseTree> &parse_tree);

  /**
   * Function to resolve hustle::parser::Expr
   */
  std::shared_ptr<Expr> resolveExpr(
      const std::shared_ptr<hustle::parser::Expr> &expr);
  /**
   * Function to resolve hustle::parser::Comparative
   */
  std::shared_ptr<Comparative> resolveComparative(
      const std::shared_ptr<hustle::parser::Comparative> &expr);

  /**
   * Function to resolve hustle::parser::ColumnReference
   */
  std::shared_ptr<ColumnReference> resolveColumnReference(
      const std::shared_ptr<hustle::parser::ColumnReference> &expr);

  /**
   * Function to resolve hustle::parser::IntLiteral
   */
  static std::shared_ptr<IntLiteral> resolveIntLiteral(
      const std::shared_ptr<hustle::parser::IntLiteral> &expr);

  /**
   * Function to resolve hustle::parser::StrLiteral
   */
  static std::shared_ptr<StrLiteral> resolveStrLiteral(
      const std::shared_ptr<hustle::parser::StrLiteral> &expr);

  /**
   * Function to resolve hustle::parser::Disjunctive
   */
  std::shared_ptr<Disjunctive> resolveDisjunctive(
      const std::shared_ptr<hustle::parser::Disjunctive> &expr);

  /**
   * Function to resolve hustle::parser::Arithmetic
   */
  std::shared_ptr<Arithmetic> resolveArithmetic(
      const std::shared_ptr<hustle::parser::Arithmetic> &expr);

  /**
   * Function to resolve hustle::parser::AggFunc
   */

  std::shared_ptr<AggFunc> resolveAggFunc(
      const std::shared_ptr<hustle::parser::AggFunc> &expr);

  /**
   * Function to return the plan
   * @return the plan
   */
  std::shared_ptr<Plan> getPlan();

  /**
   * Function to serialize the parse tree
   */
  std::string toString(int indent);

 private:
  hustle::catalog::Catalog *catalog_;
  std::shared_ptr<Plan> plan_;
  std::map<int, int> map_vir_to_real;  // matching from virtual to real table id
  std::map<int, int> map_real_to_vir;  // matching from real to virtual table id
};

}  // namespace resolver
}  // namespace hustle

#endif  // HUSTLE_RESOLVER_H
