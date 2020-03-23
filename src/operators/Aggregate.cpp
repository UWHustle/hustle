#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {

Aggregate::Aggregate(AggregateKernels aggregate_kernel,
                     std::string aggregate_column_name,
                     std::string group_by_column_name) {
    aggregate_kernel_ = aggregate_kernel;
    aggregate_column_name_ = std::move(aggregate_column_name);
    group_by_column_name_ = std::move(group_by_column_name);
}

std::shared_ptr<Table> Aggregate::run_operator
        (std::vector<std::shared_ptr<Table>> tables) {

    auto table = tables[0];
    std::shared_ptr<Table> out_table;

    if (group_by_column_name_.empty()) {
        out_table = run_operator_no_group_by(table);
    } else {
        out_table = run_operator_with_group_by(table);
    }

    return out_table;
}

std::shared_ptr<Table> Aggregate::run_operator_with_group_by
        (std::shared_ptr<Table> table) {

    arrow::Status status;
    auto group_map = get_groups(table);

    std::shared_ptr<arrow::Schema> out_schema;
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder;
    arrow::StringBuilder group_by_builder;

    // Create output schema
    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            out_schema = arrow::schema(
                    {arrow::field(group_by_column_name_, arrow::utf8()),
                     arrow::field("aggregate", arrow::int64())});
            aggregate_builder = std::make_shared<arrow::Int64Builder>();
            break;
        }
        case MEAN: {
            out_schema = arrow::schema(
                    {arrow::field(group_by_column_name_, arrow::utf8()),
                     arrow::field("aggregate", arrow::float64())});
            aggregate_builder = std::make_shared<arrow::DoubleBuilder>();
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
    }

    auto out_table = std::make_shared<Table>("aggregate", out_schema,
                                             BLOCK_SIZE);

    for (auto &key_value_pair : group_map) {

        status = group_by_builder.Append(key_value_pair.first);
        evaluate_status(status, __FUNCTION__, __LINE__);

        auto aggregate_col = table->get_column_by_name(
                aggregate_column_name_);
        auto group_indices = key_value_pair.second;

        auto aggregate = compute_aggregate(aggregate_col, group_indices);

        // Append each group aggregate to aggregate_builder.
        // TODO(nicholas): Can you condense this?
        switch (aggregate_kernel_) {
            // Returns a Datum of type INT64
            case SUM: {
                auto aggregate_builder_casted =
                        std::static_pointer_cast<arrow::Int64Builder>
                                (aggregate_builder);
                auto aggregate_casted =
                        std::static_pointer_cast<arrow::Int64Scalar>
                                (aggregate.scalar());
                status = aggregate_builder_casted->Append(
                        aggregate_casted->value);
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
            case COUNT: {
                throw std::runtime_error("Count aggregate not supported.");
                break;
            }
                // NOTE: Mean outputs a DOUBLE
            case MEAN: {
                auto aggregate_builder_casted =
                        std::static_pointer_cast<arrow::DoubleBuilder>
                                (aggregate_builder);
                auto aggregate_casted =
                        std::static_pointer_cast<arrow::DoubleScalar>
                                (aggregate.scalar());
                status = aggregate_builder_casted->Append(
                        aggregate_casted->value);
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
        }

    }

    std::shared_ptr<arrow::Array> out_aggregate_array;
    std::shared_ptr<arrow::Array> out_group_by_array;

    status = aggregate_builder->Finish(&out_aggregate_array);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = group_by_builder.Finish(&out_group_by_array);
    evaluate_status(status, __FUNCTION__, __LINE__);

    out_table->insert_records(
            {out_group_by_array->data(), out_aggregate_array->data()});
    return out_table;

}

std::shared_ptr<Table> Aggregate::run_operator_no_group_by
(std::shared_ptr<Table> table) {

    arrow::Status status;

    std::shared_ptr<arrow::Schema> out_schema;

    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::int64())});
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
            // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::float64())});
            break;
        }
    }

    auto aggregate_col = table->get_column_by_name(aggregate_column_name_);
    auto aggregate = compute_aggregate(aggregate_col, nullptr);


    std::shared_ptr<arrow::Array> out_array;
    std::shared_ptr<arrow::ArrayData> out_data;
    status = arrow::MakeArrayFromScalar(
            arrow::default_memory_pool(),
            *aggregate.scalar(),
            1,
            &out_array);
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto out_table = std::make_shared<Table>("aggregate", out_schema,
            BLOCK_SIZE);
    out_table->insert_records({out_array->data()});

    return out_table;
}

std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>
Aggregate::get_groups(std::shared_ptr<Table> table) {

    arrow::Status status;
    std::unordered_map<std::string, std::shared_ptr<arrow::Int64Builder>> hash;

    // TODO(nicholas): For now, we assume the GROUP BY column is string type.
    auto group_by_col = table->get_column_by_name(group_by_column_name_);

    for (auto &chunk :  group_by_col->chunks()) {
        auto group_by_chunk =
                std::static_pointer_cast<arrow::StringArray>(chunk);

        for (int row = 0; row < group_by_chunk->length(); row++) {
            if (hash[group_by_chunk->GetString(row)] == nullptr) {
                auto new_builder = std::make_shared<arrow::Int64Builder>();
                hash[group_by_chunk->GetString(row)] = new_builder;
            }
            status = hash[group_by_chunk->GetString(row)]->Append(row);
            evaluate_status(status, __FUNCTION__, __LINE__);
        }
    }

    std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>
            out_map;

    for (auto &key_value_pair : hash) {

        std::shared_ptr<arrow::Array> out_array;
        status = hash[key_value_pair.first]->Finish(&out_array);
        evaluate_status(status, __FUNCTION__, __LINE__);

        // The indices must be in a ChunkedArray to execute Take
        auto out_chunked_array = std::make_shared<arrow::ChunkedArray>
                (out_array);
        out_map[key_value_pair.first] = out_chunked_array;
    }

    return out_map;
}


arrow::compute::Datum Aggregate::compute_aggregate(
        std::shared_ptr<arrow::ChunkedArray> aggregate_col,
        std::shared_ptr<arrow::ChunkedArray> group_indices) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    std::shared_ptr<arrow::ChunkedArray> aggregate_group_col;

    if (group_indices == nullptr) {
        aggregate_group_col = aggregate_col;
    } else {
        status = arrow::compute::Take(
                &function_context,
                *aggregate_col,
                *group_indices,
                take_options,
                &aggregate_group_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }

    arrow::compute::Datum out_aggregate;

    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = arrow::compute::Sum(
                    &function_context,
                    aggregate_group_col,
                    &out_aggregate
            );
            break;
        }
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
                    aggregate_group_col,
                    &out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
            // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(
                    &function_context,
                    aggregate_group_col,
                    &out_aggregate
            );
            break;
        }
    }
    evaluate_status(status, __FUNCTION__, __LINE__);
    return out_aggregate;
}


} // namespace operators
} // namespace hustle
