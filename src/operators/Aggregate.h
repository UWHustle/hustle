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

struct AggregateUnit {
    AggregateKernels kernel;
    // TODO(nicholas): should table and col_name be combined into a
    //  ColumnReference?
    std::shared_ptr<Table> table;
    std::string col_name;
};


class Aggregate : public Operator{
public:

    Aggregate(
            std::shared_ptr<OperatorResult> join_result,
            std::vector<AggregateUnit> aggregate_units,
            std::vector<ColumnReference> group_bys,
            std::vector<ColumnReference> order_bys);

    std::shared_ptr<OperatorResult> run() override;



protected:

    std::shared_ptr<OperatorResult> prev_result_;

    std::vector<AggregateUnit> aggregate_refs_;
    std::vector<ColumnReference> order_bys_;
    std::vector<ColumnReference> group_bys_;

    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
    std::shared_ptr<arrow::DataType> group_type;
    std::shared_ptr<arrow::StructBuilder> group_builder;

    std::vector<std::shared_ptr<arrow::Array>> unique_values_;

    arrow::compute::Datum compute_aggregate(AggregateKernels kernel,
                                            std::shared_ptr<arrow::ChunkedArray> aggregate_col,
                                            std::shared_ptr<arrow::ChunkedArray> group_filter);

    std::shared_ptr<arrow::Schema> get_output_schema(AggregateKernels kernel);

    std::shared_ptr<arrow::ArrayBuilder>
    get_aggregate_builder(AggregateKernels kernel);


    void insert_group(int *its);

    void insert_group_aggregate(arrow::compute::Datum aggregate);

    std::shared_ptr<arrow::ChunkedArray> get_group_filter(int* its);

    std::shared_ptr<arrow::Array> get_unique_values(
            ColumnReference group_ref);

    std::shared_ptr<arrow::ChunkedArray> get_filter(
            ColumnReference col_ref,
            std::shared_ptr<arrow::Field> field,
            arrow::compute::Datum value);

    std::vector<std::shared_ptr<arrow::ArrayBuilder>> get_group_builders();
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
