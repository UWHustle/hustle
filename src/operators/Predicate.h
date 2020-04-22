//
// Created by Nicholas Corrado on 4/10/20.
//

#ifndef HUSTLE_PREDICATE_H
#define HUSTLE_PREDICATE_H

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "Operator.h"

namespace hustle::operators {


// JoinPredicate vs SelectPredicate? One has another column as the RHS, the
// other has a literal.
// Predicate
struct Predicate {
    ColumnReference col_ref_;
    arrow::compute::CompareOperator comparator_;
    arrow::compute::Datum value_;
};

struct JoinPredicate {
    ColumnReference left_col_ref_;
    arrow::compute::CompareOperator comparator_;
    ColumnReference right_col_ref_;
};

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

class PredicateNode : public Node {
public:
    explicit PredicateNode(std::shared_ptr<Predicate> predicate);
private:
};

// TODO(nicholas): Should ConjunctiveConnective and DisjunctiveConnective and
//  NotConnective inherit from this? Or should I just use this one class?
    class ConnectiveNode : public Node {
    public:
        ConnectiveNode(std::shared_ptr<Node> left_child,
                       std::shared_ptr<Node> right_child,
                       FilterOperator connective);
    protected:

    };

class PredicateTree {

public:

    explicit PredicateTree(std::shared_ptr<Node> root);
    std::shared_ptr<Node> root_;
private:

};

}








#endif //HUSTLE_PREDICATE_H
