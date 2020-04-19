//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>

namespace hustle {
namespace operators {

    OperatorResultUnit::OperatorResultUnit(
        std::shared_ptr<Table> table,
        arrow::compute::Datum filter,
        arrow::compute::Datum selection) {

        this->table = table;
        this->filter = filter;
        this->selection = selection;
    }

    OperatorResult::OperatorResult(
            std::vector<OperatorResultUnit> units) {

        units_ = units;
    }

    OperatorResult::OperatorResult(){

    }

    ResultType OperatorResult::get_type() {
        return type_;
    }

}
}