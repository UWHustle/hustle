#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

enum AggregateKernels {
  SUM,
  COUNT,
  MEAN
};

// TODO(nicholas): Add support for multiple aggregates
class AggregateOperator : public Operator{
public:
    virtual std::shared_ptr<arrow::StructArray> get_unique_values
    (std::shared_ptr<Table>
                                                    table) = 0;
    virtual std::shared_ptr<arrow::ChunkedArray> get_filter
            (std::shared_ptr<Table> table, arrow::compute::Datum value) = 0;

    arrow::compute::Datum compute_aggregate(
            std::shared_ptr<arrow::ChunkedArray> aggregate_col,
            std::shared_ptr<arrow::ChunkedArray> group_filter);

protected:
    AggregateKernels aggregate_kernel_;
    std::vector<std::shared_ptr<arrow::Field>> aggregate_fields_;
    std::vector<std::shared_ptr<arrow::Field>> group_by_fields_;
};

class AggregateComposite : public AggregateOperator {
public:

    AggregateComposite(
            std::shared_ptr<AggregateOperator> left_child,
            std::shared_ptr<AggregateOperator> right_child);

    std::shared_ptr<arrow::StructArray> get_unique_values(std::shared_ptr<Table>
                                                    table) override;
    std::shared_ptr<arrow::ChunkedArray> get_filter
            (std::shared_ptr<Table> table, arrow::compute::Datum value) override;

//    std::shared_ptr<Table> run_operator(
//            std::vector<std::shared_ptr<Table>> tables) override;


private:
    std::shared_ptr<AggregateOperator> left_child_;
    std::shared_ptr<AggregateOperator> right_child_;
};


class Aggregate : public AggregateOperator {
public:
    Aggregate(AggregateKernels aggregate_kernel,
            std::vector<std::shared_ptr<arrow::Field>> aggregate_fields,
              std::vector<std::shared_ptr<arrow::Field>> group_by_fields);

//    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>
//    get_groups(std::shared_ptr<Table> table);
    // Operator.h
    std::shared_ptr<Table> run_operator(std::vector<std::shared_ptr<Table>>
                                        tables) override;

    std::shared_ptr<Table> run_operator_no_group_by
            (std::shared_ptr<Table> table);
    std::shared_ptr<Table> run_operator_with_group_by
            (std::shared_ptr<Table> table);

    std::shared_ptr<arrow::StructArray> get_unique_values(std::shared_ptr<Table>
                                                    table) override;
    std::shared_ptr<arrow::ChunkedArray> get_filter
            (std::shared_ptr<Table> table, arrow::compute::Datum value)
            override;
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
