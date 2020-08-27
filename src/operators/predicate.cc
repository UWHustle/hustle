//
// Created by Nicholas Corrado on 4/10/20.
//

#include "predicate.h"

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
