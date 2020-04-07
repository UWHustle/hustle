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

struct JoinResultColumn {
    // TODO(nicholas): Combine into a ColumnReference?
    std::shared_ptr<Table> table;
    std::string join_col_name;
    // column AFTER a filter from a select predicate was applied.
    std::shared_ptr<arrow::ChunkedArray> join_col;
    arrow::compute::Datum filter; // filters are ChunkedArrays
    arrow::compute::Datum selection; // selections are Arrays
};

class OperatorResult {
public:
    virtual ~OperatorResult() = default;
//    virtual arrow::compute::Datum get_filter() = 0;
//    virtual arrow::compute::Datum get_indices() = 0;

    ResultType get_type();
protected:
    ResultType type_;
};

class SelectResult : public OperatorResult {
public:
    SelectResult(arrow::compute::Datum filter);

//    arrow::compute::Datum get_filter() override ;
//    arrow::compute::Datum get_indices() override ;

    arrow::compute::Datum filter_;
private:

};

class JoinResult : public OperatorResult {
public:
    JoinResult(std::vector<JoinResultColumn> join_results);

//    arrow::compute::Datum get_filter() override ;
//    arrow::compute::Datum get_indices() override ;

    std::vector<JoinResultColumn> join_results_;
private:

};

struct ColumnReference {
    std::shared_ptr<Table> table;
    std::string col_name;
};

class Operator {
    virtual std::shared_ptr<OperatorResult> run() = 0;
public:

};

class OperatorResultUnit {

public:

    OperatorResultUnit(
            std::shared_ptr<Table> table,
            std::string col_name,
            std::shared_ptr<arrow::ChunkedArray> join_col,
            arrow::compute::Datum filter,
            arrow::compute::Datum selection
    );

    // TODO(nicholas): Combine into a ColumnReference?
    std::shared_ptr<Table> table_;
    std::string col_name_;
    // column AFTER a filter from a select predicate was applied.
    std::shared_ptr<arrow::ChunkedArray> join_col;
    arrow::compute::Datum filter_; // filters are ChunkedArrays
    arrow::compute::Datum selection_; // selections are Arrays

private:

};


} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
