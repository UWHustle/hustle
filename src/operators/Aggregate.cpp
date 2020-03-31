#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {

Aggregate::Aggregate(
                     std::vector<AggregateUnit> aggregate_units,
                     std::vector<ColumnReference> group_bys,
                     std::vector<std::string> order_by_fields) {

    aggregate_units_ = aggregate_units;

    group_bys_ = std::move(group_bys);
    order_by_fields_ = std::move(order_by_fields);

    aggregate_builder_ = get_aggregate_builder(aggregate_units_[0].kernel);

    ;
    for(auto &group_by : group_bys_) {
        group_by_fields_.push_back(group_by.table->get_schema()->GetFieldByName
        (group_by.col_name));
    }
    group_type = arrow::struct_(group_by_fields_);
    group_builder = std::make_shared<arrow::StructBuilder>(
            group_type, arrow::default_memory_pool(), get_group_builders());
    
}

std::shared_ptr<Table> Aggregate::run_operator
        (std::vector<std::shared_ptr<Table>> tables) {

//    auto table = tables[0];
    return iterate_over_groups();
}

std::vector<std::shared_ptr<arrow::ArrayBuilder>>
        Aggregate::get_group_builders() {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;

    for (auto &field : group_by_fields_) {
        switch(field->type()->id()) {

            case arrow::Type::STRING: {
                group_builders.push_back(
                        std::make_shared<arrow::StringBuilder>());
                break;
            }
            case arrow::Type::INT64: {
                group_builders.push_back(
                        std::make_shared<arrow::StringBuilder>());
                break;
            }
        }
    }

    return group_builders;
}

std::shared_ptr<arrow::ArrayBuilder> Aggregate::get_aggregate_builder
(AggregateKernels kernel) {

    arrow::Status status;
    std::shared_ptr<arrow::ArrayBuilder> aggreagte_builder;

    switch (kernel) {
        // Returns a Datum of the same type INT64
        case SUM: {
            aggreagte_builder = std::make_shared<arrow::Int64Builder>();
            break;
        }
        case MEAN: {
            aggreagte_builder = std::make_shared<arrow::DoubleBuilder>();
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
    }

    return aggreagte_builder;
}

std::shared_ptr<arrow::Schema> Aggregate::get_output_schema(AggregateKernels
kernel) {

    arrow::Status status;
    arrow::SchemaBuilder schema_builder;

    status = schema_builder.AddFields(group_by_fields_);
    evaluate_status(status, __FUNCTION__, __LINE__);

    switch (kernel) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = schema_builder.AddField(
                    arrow::field("aggregate", arrow::int64()));
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case MEAN: {
            status = schema_builder.AddField(
                    arrow::field("aggregate", arrow::float64()));
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
    }

    auto result = schema_builder.Finish();
    evaluate_status(result.status(), __FUNCTION__, __LINE__);

    return result.ValueOrDie();
}


std::shared_ptr<Table> Aggregate::iterate_over_groups() {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;

    auto out_schema = get_output_schema(aggregate_units_[0].kernel);
    auto out_table = std::make_shared<Table>("aggregate", out_schema,
                                             BLOCK_SIZE);

    auto table = aggregate_units_[0].table; //TODO(nicholas)
    std::vector<std::shared_ptr<arrow::Array>> unique_values;
    // Fetch unique values for all Group By columns
    for (int i=0; i< group_by_fields_.size(); i++) {
        unique_values.push_back(
                get_unique_values(table, group_by_fields_[i]->name()));
    }

    // Initialize the slots to hold the current iteration value for each depth
    int n = group_by_fields_.size();
    int its[n];
    int maxes[n];

    for (int i = 0; i < n; i++) {
        its[i] = 0;
        maxes[i] = unique_values[i]->length();
    }

    int index = n - 1;
    bool exit = false;
    while (!exit){

        // DoSomething() loop
        auto aggregate_col = table->get_column_by_name(
                aggregate_units_[0].col_name);

        //////////
        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;
        // TODO(nicholas): for now, assume that the selections are always arrays
        //  of indices, not filters.

        auto table = aggregate_units_[0].table;
        auto selection = aggregate_units_[0].selection;
        auto name = aggregate_units_[0].col_name;

        auto col = table->get_column_by_name(name);

        status = arrow::compute::Take(&function_context,
                                      *col,
                                      *selection.make_array(),
                                      take_options,
                                      &aggregate_col);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        //////////


        auto group_filter = get_group_filter(table, unique_values, its);
        auto aggregate = compute_aggregate(aggregate_units_[0].kernel, 
                aggregate_col, group_filter);
        insert_group_aggregate(aggregate);
        insert_group(unique_values, its);

        if (n == 0) break;

        // Increment nested loop
        its[n-1]++;
        while (its[index] == maxes[index]){
            // if n == 0, we have no Group By clause and should exit after one
            // iteration.
            if (index ==  0) {
                exit = true;
                break;
            }
            its[index--] = 0;
            its[index]++;
        }
        index = n-1;
    }

    std::shared_ptr<arrow::StructArray> groups;
    status = group_builder->Finish(&groups);
    std::shared_ptr<arrow::Array> aggregates;
    status = aggregate_builder_->Finish(&aggregates);

    std::vector<std::shared_ptr<arrow::ArrayData>> table_data;
    for (int i=0; i<group_by_fields_.size(); i++) {
        table_data.push_back(groups->field(i)->data());
    }
    table_data.push_back(aggregates->data());
    out_table->insert_records(table_data);
    return out_table;
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_group_filter(
        const std::shared_ptr<Table>& table,
        std::vector<std::shared_ptr<arrow::Array>> unique_values,
        int* its) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());

    // No Group By clause
    if (group_by_fields_.empty()) {
        return nullptr;
    }

    // Fetch the first Group By filter
    auto one_unique_values =
            get_unique_values(table, group_by_fields_[0]->name());
    auto one_unique_values_casted =
            std::static_pointer_cast<arrow::StringArray>(unique_values[0]);
    arrow::compute::Datum value(
            std::make_shared<arrow::StringScalar>(
                    one_unique_values_casted->GetString(its[0])));
    auto filter = get_filter(table, group_by_fields_[0], value);

    // Fetch the next Group By filter and AND it with our current filter
    for (int field_i=1; field_i<group_by_fields_.size(); field_i++) {

        auto unique_values_casted =
                std::static_pointer_cast<arrow::StringArray>
                        (unique_values[field_i]);
        arrow::compute::Datum value(
                std::make_shared<arrow::StringScalar>(
                        unique_values_casted->GetString(its[field_i])));
        auto next_filter = get_filter(table, group_by_fields_[field_i], value);

        arrow::compute::Datum temp_filter;
        arrow::ArrayVector filter_vector;

        for (int j = 0; j < table->get_num_blocks(); j++) {

            // Note that Compare does not operate on ChunkedArrays, so we must
            // compute the filter block by block and combine them into a
            // ChunkedArray.
            status = arrow::compute::And(&function_context, filter->chunk(j),
                                         next_filter->chunk(j), &temp_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);

            filter_vector.push_back(temp_filter.make_array());
        }

        filter = std::make_shared<arrow::ChunkedArray>(filter_vector);
    }

    return filter;
}

void Aggregate::insert_group(
        std::vector<std::shared_ptr<arrow::Array>> unique_values, int *its) {

    arrow::Status status;
    for (int i=0; i<group_by_fields_.size(); i++) {
        switch(group_by_fields_[i]->type()->id()) {
            case arrow::Type::STRING: {
                auto one_group_builder = (arrow::StringBuilder*)
                        (group_builder->child(i));
//                one_group_builder->Append()
                auto one_unique_values =
                        std::static_pointer_cast<arrow::StringArray>
                                (unique_values[i]);
                status = one_group_builder->Append(one_unique_values->GetString
                (its[i]));
                evaluate_status(status, __FUNCTION__, __LINE__);
            }
        }
    }

    group_builder->Append(true);
}

void Aggregate::insert_group_aggregate(arrow::compute::Datum aggregate) {

    arrow::Status status;
    // Append each group aggregate to aggregate_builder.
    switch (aggregate.type()->id()) {
        // Returns a Datum of type INT64
        case arrow::Type::INT64: {
            auto aggregate_builder_casted =
                    std::static_pointer_cast<arrow::Int64Builder>
                            (aggregate_builder_);
            auto aggregate_casted =
                    std::static_pointer_cast<arrow::Int64Scalar>
                            (aggregate.scalar());
            // If aggregate is 0, don't include it in the output table.
            if (aggregate_casted->value == 0) {
                return;
            }
            status = aggregate_builder_casted->Append(
                    aggregate_casted->value);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case arrow::Type::DOUBLE: {
            auto aggregate_builder_casted =
                    std::static_pointer_cast<arrow::DoubleBuilder>
                            (aggregate_builder_);
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


std::shared_ptr<arrow::Array> Aggregate::get_unique_values(
        const std::shared_ptr<Table>& table,
        std::string group_by_field_name) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    std::shared_ptr<arrow::Array> unique_values;

    auto group_by_col = table->get_column_by_name(group_by_field_name);

    status = arrow::compute::Unique(&function_context, group_by_col,
                                    &unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);

    // If this field is in the Order By clause, sort it now.
    for (auto & name : order_by_fields_) {
        if (name == group_by_field_name) {

            std::shared_ptr<arrow::Array> sorted_indices;

            status = arrow::compute::SortToIndices(&function_context,
                    *unique_values, &sorted_indices);
            evaluate_status(status, __FUNCTION__, __LINE__);

            status = arrow::compute::Take(&function_context, *unique_values,
                    *sorted_indices, take_options, &unique_values);
        }
    }

    return unique_values;
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_filter
(std::shared_ptr<Table> table,
        std::shared_ptr<arrow::Field> field, arrow::compute::Datum value) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(
            arrow::compute::CompareOperator::EQUAL);
    arrow::compute::Datum filter;
    arrow::ArrayVector filter_vector;

    for (int block_id=0; block_id<table->get_num_blocks(); block_id++) {
        auto block_col = table->get_block(block_id)->get_column_by_name
                (field->name());

        // Note that Compare does not operate on ChunkedArrays, so we must
        // compute the filter block by block and combine them into a
        // ChunkedArray.
        status = arrow::compute::Compare(&function_context, block_col, value,
                compare_options, &filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        filter_vector.push_back(filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}


arrow::compute::Datum Aggregate::compute_aggregate(
        AggregateKernels kernel,
        std::shared_ptr<arrow::ChunkedArray> aggregate_col,
        std::shared_ptr<arrow::ChunkedArray> group_filter) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    std::shared_ptr<arrow::ChunkedArray> aggregate_group_col;

    if (group_filter == nullptr) {
        aggregate_group_col = aggregate_col;
    } else {
        status = arrow::compute::Filter(
                &function_context,
                *aggregate_col,
                *group_filter,
                &aggregate_group_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }

    arrow::compute::Datum out_aggregate;

    switch (kernel) {
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
