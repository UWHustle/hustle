//
// Created by Nicholas Corrado on 4/10/20.
//

#ifndef HUSTLE_PREDICATE_H
#define HUSTLE_PREDICATE_H

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "Operator.h"

namespace hustle {
namespace operators {


// JoinPredicate vs SelectPredicate? One has another column as the RHS, the
// other has a literal.
// Predicate
struct Predicate {
    ColumnReference col_ref_;
    arrow::compute::CompareOperator comparator_;
    arrow::compute::Datum value_;
};


class Node {
protected:
    bool is_leaf_node_;

public:
    bool is_leaf();
    virtual std::shared_ptr<Node> get_left_child() = 0;
    virtual std::shared_ptr<Node> get_right_child() = 0;
    virtual std::shared_ptr<Node> get_connective() = 0;
    virtual Predicate get_predicate() = 0;
};

// TODO(nicholas): Should ConjunctiveConnective and DisjunctiveConnective and
//  NotConnective inherit from this? Or should I just use this one class?
class ConnectiveNode : public Node {
public:
    ConnectiveNode(std::shared_ptr<ConnectiveNode> left_child,
                   std::shared_ptr<ConnectiveNode> right_child,
                   FilterOperator connective);

    std::shared_ptr<Node> get_left_child() override;
    std::shared_ptr<Node> get_right_child() override;
    std::shared_ptr<Node> get_connective() override;

protected:
    FilterOperator connective_;
    std::shared_ptr<ConnectiveNode> left_child_;
    std::shared_ptr<ConnectiveNode> right_child_;


};

class PredicateNode : Node {
public:
    explicit PredicateNode(Predicate predicate);
    Predicate get_predicate() override;
private:
    Predicate predicate_;
};

class PredicateTree {

    explicit PredicateTree(std::shared_ptr<Node> root);
public:
    std::shared_ptr<Node> root_;
};

}
}








#endif //HUSTLE_PREDICATE_H
