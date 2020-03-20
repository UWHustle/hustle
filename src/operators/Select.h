#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

class Select : public Operator{
 public:
  Select(
        arrow::compute::CompareOperator compare_operator,
        std::string column_name,
        arrow::compute::Datum column_value
        );

    // Operator.h
    std::shared_ptr<Table> runOperator
    (std::vector<std::shared_ptr<Table>> tables) override;

    void set_children(
            std::shared_ptr<Operator> left_child,
            std::shared_ptr<Operator> right_child,
            FilterOperator filter_operator) override;

    arrow::compute::Datum get_filter
            (std::shared_ptr<Block> block);


private:
    arrow::compute::CompareOperator compare_operator_;
    std::string column_name_;
    arrow::compute::Datum column_value_;
    FilterOperator filter_operator_;

};

class SelectComposite : Operator{
public:
    SelectComposite(
        std::shared_ptr<Select> left_child,
        std::shared_ptr<Select> right_child,
        FilterOperator filter_operator
    );

    std::shared_ptr<Table> runOperator
            (std::vector<std::shared_ptr<Table>> tables) override;


    arrow::compute::Datum get_filter
            (std::shared_ptr<Block> block);

private:
    std::shared_ptr<Select> left_child_;
    std::shared_ptr<Select> right_child_;
    FilterOperator filter_operator_;
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_SELECT_H
