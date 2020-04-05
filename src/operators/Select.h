#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

class SelectOperator : Operator {
public:
    virtual arrow::compute::Datum get_filter(std::shared_ptr<Block> block) = 0;
};

class Select : public SelectOperator{
public:

    Select(
        arrow::compute::CompareOperator compare_operator,
        std::string column_name,
        arrow::compute::Datum column_value
        );
    arrow::compute::Datum select(std::shared_ptr<Table> table);
    std::shared_ptr<OperatorResult> run() override;


private:
//    std::shared_ptr<Table> table_;
    arrow::compute::CompareOperator compare_operator_;
    std::string column_name_;
    arrow::compute::Datum column_value_;

    arrow::compute::Datum get_filter
            (std::shared_ptr<Block> block) override;
    arrow::compute::Datum get_filter
            (std::shared_ptr<Table> table);
};

class SelectComposite : public SelectOperator{
public:
    SelectComposite(
        std::shared_ptr<SelectOperator> left_child,
        std::shared_ptr<SelectOperator> right_child,
        FilterOperator filter_operator
    );

    arrow::compute::Datum select(std::shared_ptr<Table> table);
    std::shared_ptr<OperatorResult> run() override;


private:
    std::shared_ptr<Table> table_;
    std::shared_ptr<SelectOperator> left_child_;
    std::shared_ptr<SelectOperator> right_child_;
    FilterOperator filter_operator_;

    arrow::compute::Datum get_filter
            (std::shared_ptr<Block> block) override;

        arrow::compute::Datum get_filter
                (std::shared_ptr<Table> table);
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_SELECT_H
