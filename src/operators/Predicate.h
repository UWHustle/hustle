//
// Created by Nicholas Corrado on 4/10/20.
//

#ifndef HUSTLE_PREDICATE_H
#define HUSTLE_PREDICATE_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

struct Predicate {
    ColumnReference col_ref;
    arrow::compute::CompareOperator comparator;
    arrow::compute::Datum value;
};

struct PredicateNode {

    Predicate predicate;
    std::shared_ptr<PredicateNode> left_child;
    std::shared_ptr<PredicateNode> right_child;

};

class Predicate {

public:

    std::shared_ptr<PredicateNode> root;

};






#endif //HUSTLE_PREDICATE_H
