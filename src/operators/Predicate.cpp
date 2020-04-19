//
// Created by Nicholas Corrado on 4/10/20.
//

#include "Predicate.h"

namespace hustle {
namespace operators {

    SimplePredicate::SimplePredicate(ColumnReference col_ref,
                                     arrow::compute::CompareOperator comparator,
                                     arrow::compute::Datum value) {
        
        col_ref_ = col_ref;
        comparator_ = comparator;
        value_ = value;
    }

    bool SimplePredicate::has_children() {
        return false;
    }

    ConnectivePredicate::ConnectivePredicate(
            std::shared_ptr<Predicate> left_child,
            std::shared_ptr<Predicate> right_child) {

        left_child_ = left_child;
        right_child_ = right_child;
    }

    bool ConnectivePredicate::has_children() {
        return true;
    }
}
}
