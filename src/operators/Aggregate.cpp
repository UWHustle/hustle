#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>


namespace hustle {
namespace operators {

Aggregate::Aggregate(AggregateKernels aggregate_kernel,
                     std::string aggregate_column_name,
                     std::string group_by_column_name) {
    aggregate_kernel_ = aggregate_kernel;
    aggregate_column_name_ = std::move(aggregate_column_name);
    group_by_column_name_ = std::move(group_by_column_name);
}

std::unordered_map<std::string, arrow::compute::Datum> Aggregate::get_groups
(std::shared_ptr<Table> table) {

    arrow::Status status;
    std::unordered_map<std::string, std::shared_ptr<arrow::Int64Builder>> hash;

    // TODO(nicholas): For now, we assume the GROUP BY column is string type.
    auto group_by_col = table->get_column_by_name(group_by_column_name_);

    for (auto &chunk :  group_by_col->chunks()) {
        auto group_by_chunk =
                std::static_pointer_cast<arrow::StringArray>(chunk);

        for (int row=0; row<group_by_chunk->length(); row++) {
            auto int64_builder = hash[group_by_chunk->GetString(row)];
            status = int64_builder->Append(row);
            evaluate_status(status, __FUNCTION__, __LINE__);
        }
    }

    std::unordered_map<std::string, arrow::compute::Datum> out_map;

    for (auto &key_value_pair : hash) {

        std::shared_ptr<arrow::Array> out_array;
        status = hash[key_value_pair.first]->Finish(&out_array);
        evaluate_status(status, __FUNCTION__, __LINE__);

        arrow::compute::Datum out_datum(out_array);
        out_map[key_value_pair.first] = out_datum;
    }

    return out_map;
}

std::shared_ptr<Table> Aggregate::run_operator
(std::vector<std::shared_ptr<Table>> tables) {
    // operator only uses first table, ignore others
    auto table = tables[0];

    if (table == nullptr) {
        return nullptr;
    }
    //
    arrow::Status status;

    auto full_column = table->get_column_by_name(aggregate_column_name_);

    if (full_column == nullptr) {
        return nullptr;
    }
    auto* memory_pool = arrow::default_memory_pool();
    arrow::compute::FunctionContext function_context(memory_pool);
    arrow::compute::Datum out_aggregate;

    std::shared_ptr<arrow::Schema> out_schema;

    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = arrow::compute::Sum(
                    &function_context,
                    full_column,
                    &out_aggregate
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
                    &out_aggregate
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
                    &out_aggregate
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
            *out_aggregate.scalar(),
            1,
            &out_array);
    evaluate_status(status, __FUNCTION__, __LINE__);


    auto out_table = std::make_shared<Table>("aggregate", out_schema,
            BLOCK_SIZE);
    auto test = out_table->get_schema()->field_names();

    out_table->insert_records({out_array->data()});

    return out_table;
}

//    void Aggregate::set_children(std::shared_ptr<Operator> left_child,
//                                 std::shared_ptr<Operator> right_child,
//                                 FilterOperator filter_operator) {
//
//    }


} // namespace operators
} // namespace hustle
