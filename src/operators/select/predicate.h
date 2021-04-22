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

#ifndef HUSTLE_PREDICATE_H
#define HUSTLE_PREDICATE_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "operators/utils/operator_result.h"

namespace hustle::operators {

enum CompareOperator {
  EQUAL,
  NOT_EQUAL,
  LESS,
  LESS_EQUAL,
  GREATER,
  GREATER_EQUAL,
  BETWEEN
};

struct Predicate {
  ColumnReference col_ref_;
  arrow::compute::CompareOperator comparator_;
  arrow::Datum value_;
  arrow::Datum value2_;
};

struct JoinPredicate {
  ColumnReference left_col_;
  arrow::compute::CompareOperator comparator_;
  ColumnReference right_col_;

  bool operator==(const JoinPredicate& join_predicate) const {
    return (left_col_ == join_predicate.left_col_ &&
            right_col_ == join_predicate.right_col_) &&
           comparator_ == join_predicate.comparator_;
  }

  bool operator!=(const JoinPredicate& join_predicate) const {
    return (left_col_ != join_predicate.left_col_ ||
            right_col_ != join_predicate.right_col_) ||
           comparator_ != join_predicate.comparator_;
  }
};

struct OrderByReference {
  ColumnReference col_ref_;
  bool is_desc;
};

using JoinPredicatePtr = std::shared_ptr<JoinPredicate>;

/**
 * The base class for nodes of a PredicateTree. Internal nodes contain a
 * connective operator (AND, OR, NOT) and leaf nodes contain a Predicate.
 * Since different node types contain different data types, I include all
 * possible data types in the Node class. The is_leaf() function should be
 * called beforehand to determine which data members are valid.
 */
class Node {
 protected:
  bool is_leaf_node_;

 public:
  bool is_leaf();

  // Should only be access if is_leaf() is false
  FilterOperator connective_;
  std::shared_ptr<Node> left_child_;
  std::shared_ptr<Node> right_child_;

  // Should only be access if is_leaf() is true
  std::shared_ptr<Predicate> predicate_;

  void print() {
    if (is_leaf()) {
      std::cout << predicate_->col_ref_.col_name
                << " comp:" << predicate_->comparator_ << " "
                << "val " << predicate_->value_.ToString() << " "
                << predicate_->value2_.ToString() << std::endl;
      return;
    }
    left_child_->print();
    std::cout << "CONNECTIVE: " << connective_ << std::endl;
    right_child_->print();
  }
};

/**
 * A leaf node of a PredicateTree
 */
class PredicateNode : public Node {
 public:
  explicit PredicateNode(std::shared_ptr<Predicate> predicate);

 private:
};

// TODO(nicholas): Should ConjunctiveConnective and DisjunctiveConnective
//  and NotConnective inherit from this? Or should I just use this one
//  class?
/**
 * An internal node of a PredicateTree
 */
class ConnectiveNode : public Node {
 public:
  ConnectiveNode(std::shared_ptr<Node> left_child,
                 std::shared_ptr<Node> right_child, FilterOperator connective);
};

/**
 * A binary tree of Nodes. Internal nodes are ConnectiveNodes and leaf nodes
 * are PredicateNodes
 */
class PredicateTree {
 public:
  explicit PredicateTree(std::shared_ptr<Node> root);
  std::shared_ptr<Node> root_;
  int table_id_;
  std::string table_name_;
};

}  // namespace hustle::operators

#endif  // HUSTLE_PREDICATE_H
