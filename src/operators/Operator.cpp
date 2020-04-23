//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>

namespace hustle {
namespace operators {

    LazyTable::LazyTable(
        std::shared_ptr<Table> table,
        arrow::compute::Datum filter,
        arrow::compute::Datum selection) {

        this->table = table;
        this->filter = filter;
        this->selection = selection;
    }

    OperatorResult::OperatorResult(
            std::vector<LazyTable> units) {

        lazy_tables_ = units;
    }

    OperatorResult::OperatorResult(){

    }

    ResultType OperatorResult::get_type() {
        return type_;
    }

    void OperatorResult::add_table(std::shared_ptr<Table> table) {
        LazyTable lazy_table(
                table,
                arrow::compute::Datum(),
                arrow::compute::Datum());
        lazy_tables_.insert(lazy_tables_.begin(),lazy_table);
    }

    void OperatorResult::append(std::shared_ptr<OperatorResult> result) {
        for (auto &lazy_table : result->lazy_tables_) {
            lazy_tables_.push_back(lazy_table);
        }
    }

}
}