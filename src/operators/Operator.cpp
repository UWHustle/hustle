//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>

namespace hustle {
namespace operators {

    ResultType OperatorResult::get_type() {
        return type_;
    }

    JoinResult::JoinResult(std::vector <JoinResultColumn> join_results) {
        join_results_ = join_results;
        type_ = JOIN;
    }

    SelectResult::SelectResult(arrow::compute::Datum filter) {
        filter_ = filter;
        type_ = SELECT;
    }
}
}