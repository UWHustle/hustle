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
    std::shared_ptr<Table> table;
    arrow::compute::Datum filter;
    arrow::compute::Datum selection;
    std::string col_name;
};


class Aggregate : public Operator{
public:

    Aggregate(
            std::vector<JoinResult> join_result,
             std::vector<AggregateUnit> Aggregate_units,
             std::vector<ColumnReference> group_by_fields,
             std::vector<std::string> order_by_fields);

    std::shared_ptr<Table> aggregate();

protected:
    arrow::compute::Datum selection_;
    std::vector<JoinResult> join_result_;

    std::vector<ColumnReference> group_bys_;
    std::vector<std::shared_ptr<arrow::Field>> group_by_fields_;
    std::vector<std::string> order_by_names_;
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
    std::shared_ptr<arrow::DataType> group_type;
    std::shared_ptr<arrow::StructBuilder> group_builder;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_by_builders_;

    std::vector<AggregateUnit> aggregate_units_;

    arrow::compute::Datum compute_aggregate(AggregateKernels kernel,
                                            std::shared_ptr<arrow::ChunkedArray> aggregate_col,
                                            std::shared_ptr<arrow::ChunkedArray> group_filter);

    std::shared_ptr<arrow::Schema> get_output_schema(AggregateKernels kernel);

    std::shared_ptr<arrow::ArrayBuilder>
    get_aggregate_builder(AggregateKernels kernel);

    void insert_group(
            std::vector<std::shared_ptr<arrow::Array>> unique_values, int *its);
    void insert_group_aggregate(arrow::compute::Datum aggregate);

    std::shared_ptr<arrow::ChunkedArray> get_group_filter(
            std::vector<std::shared_ptr<arrow::Array>> unique_values,
            int* its);

    std::shared_ptr<Table> iterate_over_groups
            ();

    std::shared_ptr<arrow::Array> get_unique_values(
            ColumnReference group_ref);
    std::shared_ptr<arrow::ChunkedArray> get_filter
            (ColumnReference col_ref,
             std::shared_ptr<arrow::Field> field, arrow::compute::Datum value);

    std::vector<std::shared_ptr<arrow::ArrayBuilder>>
    get_group_builders();
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
