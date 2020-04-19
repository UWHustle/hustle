//
// Created by Nicholas Corrado on 4/10/20.
//

#include "Predicate.h"

#include <utility>

namespace hustle {
namespace operators {


    bool Node::is_leaf() {
        return is_leaf_node_;
    }

    PredicateNode::PredicateNode(Predicate predicate) {
        predicate_ = std::move(predicate);
    }

    Predicate PredicateNode::get_predicate() {
        return Node::get_predicate();
    }


    ConnectiveNode::ConnectiveNode(std::shared_ptr<ConnectiveNode> left_child,
                                   std::shared_ptr<ConnectiveNode> right_child,
                                   FilterOperator connective) {

        left_child_ = std::move(left_child);
        right_child_ = std::move(right_child);
        connective_ = connective;
    }

    std::shared_ptr<Node> ConnectiveNode::get_right_child() {
        return Node::get_right_child();
    }

    std::shared_ptr<Node> ConnectiveNode::get_left_child() {
        return Node::get_left_child();
    }

    std::shared_ptr<Node> ConnectiveNode::get_connective() {
        return Node::get_connective();
    }

    PredicateTree::PredicateTree(std::shared_ptr<Node> root) {
        root_ = root;
    }
}
}
