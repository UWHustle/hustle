#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "LazyTable.h"

namespace hustle::operators {

enum FilterOperator {
    AND,
    OR,
    NONE,
};

class OperatorResult {
public:
    OperatorResult();
    explicit OperatorResult(std::vector<LazyTable> units);
    void append(std::shared_ptr<Table> table);
    void append(const std::shared_ptr<OperatorResult>& result);
    LazyTable get_table(int i);
    LazyTable get_table(const std::shared_ptr<Table>& table);

    std::shared_ptr<Table> materialize(const std::vector<ColumnReference>& col_refs);

    std::vector<LazyTable> lazy_tables_;
};

class Operator {
    virtual std::shared_ptr<OperatorResult> run() = 0;
public:

};




} // namespace hustle

#endif //HUSTLE_OPERATOR_H
