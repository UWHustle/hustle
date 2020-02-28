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

    std::shared_ptr<arrow::ChunkedArray> get_filter
    (std::shared_ptr<Table> table);

    arrow::compute::Datum* get_filter
            (std::shared_ptr<Block> block);


private:
  arrow::compute::CompareOperator compare_operator_;
  std::string column_name_;
  arrow::compute::Datum column_value_;

};

class SelectComposite : public Select{
public:
    SelectComposite(
        arrow::compute::CompareOperator compare_operator,
        std::string column_name,
        arrow::compute::Datum column_value,
        std::shared_ptr<Operator> left_child,
        std::shared_ptr<Operator> right_child,
        FilterOperator filter_operator
    );

private:
    std::shared_ptr<Operator> left_child;
    std::shared_ptr<Operator> right_child;
    FilterOperator filter_operator;
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_SELECT_H
