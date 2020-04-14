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
struct Predicate {
    // The LHS side of the predicate (i.e. the column) will be provided
    // inside the operator.
    ColumnReference col_ref;
    arrow::compute::CompareOperator comparator;
    arrow::compute::Datum value;
};

struct PredicateNode {

    Predicate predicate;
    std::shared_ptr<PredicateNode> left_child;
    std::shared_ptr<PredicateNode> right_child;

};

class PredicateTree {

public:

    std::shared_ptr<PredicateNode> root;

};


}
}








#endif //HUSTLE_PREDICATE_H
