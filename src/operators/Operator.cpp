//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>

namespace hustle {
namespace operators {

    JoinResult::JoinResult(std::vector <JoinResultColumn> join_results) {
        join_results_ = std::move(join_results);
        type_ = JOIN;
    }

    SelectResult::SelectResult(arrow::compute::Datum filter) {
        filter_ = std::move(filter);
        type_ = SELECT;
    }

}
}