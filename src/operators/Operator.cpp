//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>

namespace hustle {
namespace operators {

    OperatorResultUnit::OperatorResultUnit(
        std::shared_ptr<Table> table,
        std::string col_name,
        std::shared_ptr<arrow::ChunkedArray> join_col,
        arrow::compute::Datum filter,
        arrow::compute::Datum selection) {

        table_ = table;
        col_name_ = col_name;
        filter_ = filter;
        selection_ = selection;
    }

    ResultType OperatorResult::get_type() {
        return type_;
    }

    JoinResult::JoinResult(std::vector <JoinResultColumn> join_results) {
        join_results_ = join_results;
        type_ = JOIN;
    }

//    arrow::compute::Datum JoinResult::get_filter() {
//        return
//    }

    SelectResult::SelectResult(arrow::compute::Datum filter) {
        filter_ = filter;
        type_ = SELECT;
    }

//    arrow::compute::Datum SelectResult::get_indices() {
//        return arrow::compute::Datum();
//    }
}
}