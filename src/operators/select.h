
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

#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <arrow/compute/api.h>

#include <string>

#include "operators/join.h"
#include "operators/operator.h"
#include "operators/predicate.h"
#include "operators/utils/operator_result.h"
#include "storage/block.h"
#include "storage/table.h"

namespace hustle::operators {

/**
 * The Select operator updates the filter of a LazyTable so that it filters out
 * all tuples that do not satisfy the selection predicate.
 *
 * Predicates are inputted as a predicate tree. All internal nodes of the tree
 * are connective operators (AND, OR), while leaf nodes are simple predicates,
 * e.g. column = 7.
 */
class Select : public Operator {
 public:
  /**
   * Construct a Select operator.
   *
   * @param prev_result OperatorResult from an upstream operator
   * @param tree predicate tree
   */
  Select(const std::size_t query_id, std::shared_ptr<Table> table,
         std::shared_ptr<OperatorResult> prev_result,
         std::shared_ptr<OperatorResult> output_result,
         std::shared_ptr<PredicateTree> tree);

  /**
   * Perform the selection specified by the predicate tree passed into the
   * constructor.
   *
   * @return a new OperatorResult with an updated filter.
   */
  void execute(Task *ctx) override;

 private:
  std::shared_ptr<Table> table_;
  std::shared_ptr<OperatorResult> output_result_;
  std::shared_ptr<PredicateTree> tree_;
  arrow::ArrayVector filters_;

  void ExecuteBlock(int block_index);

  /**
   * Perform the selection specified by a node in the predicate tree on
   * one block of the table. If the node is a not a leaf node, this
   * function will be recursively called on its children. The nodes of the
   * predicate tree are visited using inorder traversal.
   *
   * @param block A block of the table
   * @param node A node of the predicate tree.
   *
   * @return A filter corresponding to values that satisfy the node's
   * selection predicate(s)
   */
  arrow::Datum Filter(const std::shared_ptr<Block> &block,
                      const std::shared_ptr<Node> &node);

  /**
   * Perform the selection specified by a predicate (i.e. leaf node) in the
   * predicate tree on one block of the table. This is the base function
   * call of the other get_filter() function.
   *
   * @param block A block of the table
   * @param predicate A predicate from one of the leaf nodes of the
   * predicate tree.
   *
   * @return A filter corresponding to values that satisfy the node's
   * selection predicate(s)
   */
  arrow::Datum Filter(const std::shared_ptr<Block> &block,
                      const std::shared_ptr<Predicate> &predicate);

  template <typename T, typename Op>
  arrow::Datum Filter(const std::shared_ptr<Block> &block,
                      const ColumnReference &col_ref, const T &value,
                      Op comparator);
};

}  // namespace hustle::operators

#endif  // HUSTLE_SELECT_H
