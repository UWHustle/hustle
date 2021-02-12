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

#include "operators/select/predicate.h"

#include <utility>

namespace hustle::operators {

bool Node::is_leaf() { return is_leaf_node_; }

PredicateNode::PredicateNode(std::shared_ptr<Predicate> predicate) {
  predicate_ = std::move(predicate);
  is_leaf_node_ = true;
}

ConnectiveNode::ConnectiveNode(std::shared_ptr<Node> left_child,
                               std::shared_ptr<Node> right_child,
                               FilterOperator connective) {
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
  connective_ = connective;

  is_leaf_node_ = false;
}

PredicateTree::PredicateTree(std::shared_ptr<Node> root) {
  root_ = std::move(root);
}
}  // namespace hustle::operators
