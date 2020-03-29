#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"
#include "Project.h"

namespace hustle {
namespace operators {

enum AggregateKernels {
  SUM,
  COUNT,
  MEAN
};

class Aggregate : public Operator{
public:

    Aggregate(AggregateKernels aggregate_kernel,
                         std::vector<ProjectionUnit> projection_units,
                         std::vector<std::shared_ptr<arrow::Field>> group_by_fields,
                         std::vector<std::shared_ptr<arrow::Field>>
                         order_by_fields);

    std::shared_ptr<Table> run_operator(std::vector<std::shared_ptr<Table>>
                                        tables) override;

    void insert_group(
            std::vector<std::shared_ptr<arrow::Array>> unique_values, int *its);
    void insert_group_aggregate(arrow::compute::Datum aggregate);

    std::shared_ptr<arrow::ChunkedArray> get_group_filter(
            const std::shared_ptr<Table>& table,
            std::vector<std::shared_ptr<arrow::Array>> unique_values,
            int* its);

    std::shared_ptr<Table> iterate_over_groups
            ();

    std::shared_ptr<arrow::Array> get_unique_values(const std::shared_ptr<Table>&
    table, std::string);
    std::shared_ptr<arrow::ChunkedArray> get_filter
            (std::shared_ptr<Table> table,
             std::shared_ptr<arrow::Field> field, arrow::compute::Datum value);

    arrow::compute::Datum compute_aggregate(
            std::shared_ptr<arrow::ChunkedArray> aggregate_col,
            std::shared_ptr<arrow::ChunkedArray> group_filter);

    std::shared_ptr<arrow::Schema> get_output_schema();
    std::shared_ptr<arrow::ArrayBuilder> get_aggregate_builder();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>>
    get_group_builders();

protected:
    AggregateKernels aggregate_kernel_;
    std::vector<std::shared_ptr<arrow::Field>> group_by_fields_;
    std::vector<std::shared_ptr<arrow::Field>> order_by_fields_;
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder_;
    std::shared_ptr<arrow::DataType> group_type;
    std::shared_ptr<arrow::StructBuilder> group_builder;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_by_builders_;

    std::vector<ProjectionUnit> projection_units_;
};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
