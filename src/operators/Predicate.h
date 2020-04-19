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
class Predicate {
public:
    virtual bool has_children() = 0;
};

class SimplePredicate : Predicate {
public:
    SimplePredicate(ColumnReference col_ref,
            arrow::compute::CompareOperator comparator,
            arrow::compute::Datum value);

private:
    bool has_children() override;

    ColumnReference col_ref_;
    arrow::compute::CompareOperator comparator_;
    arrow::compute::Datum value_;
};

class ConnectivePredicate : Predicate {
public:
    ConnectivePredicate(std::shared_ptr<Predicate> left_child,
            std::shared_ptr<Predicate> right_child);
private:

    bool has_children() override;

    std::shared_ptr<Predicate> left_child_;
    std::shared_ptr<Predicate> right_child_;
};

}
}








#endif //HUSTLE_PREDICATE_H
