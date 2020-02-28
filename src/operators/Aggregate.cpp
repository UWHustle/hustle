#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>

namespace hustle {
namespace operators {

Aggregate::Aggregate(AggregateKernels aggregate_kernel,
                     std::string column_name) {
    aggregate_kernel_ = aggregate_kernel;
    column_name_ = std::move(column_name);
}

std::shared_ptr<Table> Aggregate::runOperator
(std::vector<std::shared_ptr<Table>> tables) {
    // operator only uses first table, ignore others
    auto table = tables[0];
    //
    arrow::Status status;

    auto full_column = table->get_column_by_name(column_name_);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    std::vector<arrow::compute::Datum*> out_aggregates;
    auto *out_aggregate = new arrow::compute::Datum();

    std::shared_ptr<arrow::Schema> out_schema;

    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = arrow::compute::Sum(
                    &function_context,
                    full_column,
                    out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            out_schema = arrow::schema({arrow::field("aggregate",
                                        arrow::int64())});
        }
            break;
        // Returns a Datum of the same type as the column
        case COUNT: {
            // TODO(martin): count options
            // TODO(nicholas): Currently, Count cannot accept
            //  ChunkedArray Datums. Support for ChunkedArray Datums
            //  was recently added (late January) for Sum and Mean,
            //  and it seems reasonable to assume that it will be
            //  implemented for Count soon too. Once support is
            //  added, we can remove the line below.
            throw std::runtime_error("Count aggregate not supported.");
            auto count_options = arrow::compute::CountOptions(
                    arrow::compute::CountOptions::COUNT_ALL);
            status = arrow::compute::Count(
                    &function_context,
                    count_options,
                    full_column,
                    out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::int64())});
        }
            break;
        // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(
                    &function_context,
                    full_column,
                    out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::float64())});
        }
            break;
    }
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::Array> out_array;
    std::shared_ptr<arrow::ArrayData> out_data;
    status = arrow::MakeArrayFromScalar(
            arrow::default_memory_pool(),
            *out_aggregate->scalar(),
            1,
            &out_array);
    evaluate_status(status, __FUNCTION__, __LINE__);


    auto out_table = std::make_shared<Table>("aggregate", out_schema,
            BLOCK_SIZE);
    auto test = out_table->get_schema()->field_names();

    out_table->insert_records({out_array->data()});

    return out_table;
}

} // namespace operators
} // namespace hustle
