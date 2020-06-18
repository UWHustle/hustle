//
// Created by Nicholas Corrado on 4/10/20.
//

#ifndef HUSTLE_PREDICATE_H
#define HUSTLE_PREDICATE_H

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "OperatorResult.h"

namespace hustle::operators {


struct Predicate {
    ColumnReference col_ref_;
    arrow::compute::CompareOperator comparator_;
    arrow::Datum value_;
};

struct JoinPredicate {
    ColumnReference left_col_ref_;
    arrow::compute::CompareOperator comparator_;
    ColumnReference right_col_ref_;
};

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
                   std::shared_ptr<Node> right_child,
                   FilterOperator connective);
};

/**
 * A binary tree of Nodes. Internal nodes are ConnectiveNodes and leaf nodes
 * are PredicateNodes
 */
class PredicateTree {
public:
    explicit PredicateTree(std::shared_ptr<Node> root);
    std::shared_ptr<Node> root_;
};

}








#endif //HUSTLE_PREDICATE_H
