#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

namespace hustle {
namespace operators {

enum FilterOperator {
    AND,
    OR,
    NONE
};


enum ResultType {
    SELECT,
    JOIN,
    AGGREGATE,
    PROJECT,
};


    class OperatorResultUnit {

    public:

        OperatorResultUnit(
                std::shared_ptr<Table> table,
                arrow::compute::Datum filter,
                arrow::compute::Datum selection
        );

        // TODO(nicholas): Combine into a ColumnReference?
        std::shared_ptr<Table> table;
        arrow::compute::Datum filter; // filters are ChunkedArrays
        arrow::compute::Datum selection; // selections are Arrays

    private:

    };

class OperatorResult {
public:
//    virtual arrow::compute::Datum get_filter() = 0;
//    virtual arrow::compute::Datum get_indices() = 0;
//    OperatorResult() = default;
    OperatorResult();
    OperatorResult(std::vector<OperatorResultUnit> units);

    std::vector<OperatorResultUnit> units_;
    ResultType get_type();
protected:
    ResultType type_;
};

struct ColumnReference {
    std::shared_ptr<Table> table;
    std::string col_name;
};

class Operator {
    virtual std::shared_ptr<OperatorResult> run() = 0;
public:

};




} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
