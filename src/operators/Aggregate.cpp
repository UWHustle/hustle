
#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>


namespace hustle::operators {

Aggregate::Aggregate(
    const std::size_t query_id,
    std::shared_ptr<OperatorResult> prev_result,
    std::shared_ptr<OperatorResult> output_result,
    std::vector<AggregateReference> aggregate_refs,
    std::vector<ColumnReference> group_refs,
    std::vector<ColumnReference> order_by_refs) :
    Operator(query_id) {

    prev_result_ = prev_result;
    output_result_ = output_result;
    aggregate_refs_ = aggregate_refs;

    group_by_refs_ = std::move(group_refs);
    order_by_refs_ = std::move(order_by_refs);

    // Fetch the fields associated with each groupby column.
    std::vector<std::shared_ptr<arrow::Field>> group_by_fields;
    for (auto &group_by : group_by_refs_) {
        group_by_fields.push_back(
            group_by.table->get_schema()
                ->GetFieldByName(group_by.col_name));
    }

    // Initialize a StructBuilder containing one builder for each group
    // by column.
    groupt_type_ = arrow::struct_(group_by_fields);
    group_builder_ = std::make_shared<arrow::StructBuilder>(
        groupt_type_, arrow::default_memory_pool(), get_group_builders());

    // Initialize aggregate builder
    aggregate_builder_ = get_aggregate_builder(aggregate_refs_[0].kernel);

    // Initialize output table schema. group_type_ must be initialized beforehand.
    out_schema_ = get_output_schema(aggregate_refs_[0].kernel,
                                    aggregate_refs_[0].agg_name);
    //Initialize output table.
    out_table_ = std::make_shared<Table>("aggregate", out_schema_, BLOCK_SIZE);

}


std::vector<std::shared_ptr<arrow::ArrayBuilder>>
Aggregate::get_group_builders() {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
    for (auto &field : groupt_type_->children()) {
        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                group_builders.push_back(
                    std::make_shared<arrow::StringBuilder>());
                break;
            }
            case arrow::Type::INT64: {
                group_builders.push_back(
                    std::make_shared<arrow::Int64Builder>());
                break;
            }
            default: {
                std::cerr << "Aggregate does not support group bys of type "
                             + field->type()->ToString() << std::endl;
            }
        }
    }

    return group_builders;
}

std::shared_ptr<arrow::ArrayBuilder> Aggregate::get_aggregate_builder(
    AggregateKernels kernel) {

    arrow::Status status;
    std::shared_ptr<arrow::ArrayBuilder> aggreagte_builder;

    switch (kernel) {
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

std::shared_ptr<arrow::Schema> Aggregate::get_output_schema(
    AggregateKernels kernel,
    std::string agg_col_name) {

    arrow::Status status;
    arrow::SchemaBuilder schema_builder;

    if (groupt_type_ != nullptr) {
        status = schema_builder.AddFields(groupt_type_->children());
        evaluate_status(status, __FUNCTION__, __LINE__);
    }
    switch (kernel) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = schema_builder.AddField(
                arrow::field(agg_col_name, arrow::int64()));
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case MEAN: {
            status = schema_builder.AddField(
                arrow::field(agg_col_name, arrow::float64()));
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

std::shared_ptr<arrow::ChunkedArray>
Aggregate::get_group_filter(std::vector<int> group_id) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());

    // No Group By clause
    if (groupt_type_->num_children() == 0) {
        return nullptr;
    }

    arrow::compute::Datum value;
    std::shared_ptr<arrow::ChunkedArray> prev_filter;

    // Fetch the next Group By filter and AND it with the previous filter
    for (int field_i = 0; field_i < groupt_type_->num_children(); field_i++) {

        std::shared_ptr<arrow::ChunkedArray> next_filter;

        switch (groupt_type_->child(field_i)->type()->id()) {
            case arrow::Type::STRING: {
                auto one_unique_values_casted =
                    std::static_pointer_cast<arrow::StringArray>
                        (all_unique_values_[field_i]);
                value = arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>(
                        one_unique_values_casted->GetString(
                            group_id[field_i])));
                next_filter = get_unique_value_filter(group_by_refs_[field_i],
                                                      value);
                break;
            }
            case arrow::Type::INT64: {
                auto one_unique_values_casted =
                    std::static_pointer_cast<arrow::Int64Array>
                        (all_unique_values_[field_i]);
                value = arrow::compute::Datum(
                    std::make_shared<arrow::Int64Scalar>(
                        one_unique_values_casted->Value(group_id[field_i])));
                next_filter = get_unique_value_filter(group_by_refs_[field_i],
                                                      value);
                break;
            }
            default: {
                std::cerr << "invalid type" << std::endl;
            }
        }

        arrow::compute::Datum temp_filter;
        arrow::ArrayVector filter_vector;

        if (prev_filter != nullptr) {
            for (int j = 0; j < prev_filter->num_chunks(); j++) {

                // Note that Compare cannot operate on ChunkedArrays.
                status = arrow::compute::And(&function_context,
                                             prev_filter->chunk(j),
                                             next_filter->chunk(j),
                                             &temp_filter);
                evaluate_status(status, __FUNCTION__, __LINE__);

                filter_vector.push_back(temp_filter.make_array());
            }

            prev_filter = std::make_shared<arrow::ChunkedArray>
                (filter_vector);
        } else {
            prev_filter = next_filter;
        }
    }

    return prev_filter;
}

void Aggregate::insert_group(std::vector<int> group_id) {

    arrow::Status status;
    // Loop over columns in group builder, and append one of its unique values to
    // its builder.
    for (int i = 0; i < groupt_type_->num_children(); i++) {
        switch (groupt_type_->child(i)->type()->id()) {

            case arrow::Type::STRING: {
                // Downcast the column's builder
                auto col_group_builder = (arrow::StringBuilder *)
                    (group_builder_->child(i));
                // Downcast the column's unique_values
                auto col_unique_values =
                    std::static_pointer_cast<arrow::StringArray>
                        (all_unique_values_[i]);

                // Append one of the unique values to the column's builder.
                status = col_group_builder->Append(
                    col_unique_values->GetString(group_id[i]));
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
            case arrow::Type::INT64: {
                // Downcast the column's builder
                auto col_group_builder = (arrow::Int64Builder *)
                    (group_builder_->child(i));
                // Downcast the column's unique_values
                auto col_unique_values =
                    std::static_pointer_cast<arrow::Int64Array>
                        (all_unique_values_[i]);

                // Append one of the unique values to the column's builder.
                status = col_group_builder->Append(
                    col_unique_values->Value(group_id[i]));
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
            default: {
                std::cerr << "Cannot insert unsupported aggregate type: "
                             + groupt_type_->child(i)->type()->ToString()
                          << std::endl;
            }
        }
    }
    // StructBuilder does not automatically update its length when we append to
    // its children. We must do this manually.
    status = group_builder_->Append(true);
    evaluate_status(status, __FUNCTION__, __LINE__);
}

void Aggregate::insert_group_aggregate(arrow::compute::Datum aggregate) {

    arrow::Status status;
    // Append a group's aggregate to its builder.
    switch (aggregate.type()->id()) {
        case arrow::Type::INT64: {
            // Downcast the aggregate builder
            auto aggregate_builder_casted =
                std::static_pointer_cast<arrow::Int64Builder>
                    (aggregate_builder_);
            // Downcast the group aggregate
            auto aggregate_casted =
                std::static_pointer_cast<arrow::Int64Scalar>
                    (aggregate.scalar());
            // If aggregate == 0, don't include it in the output table.
            if (aggregate_casted->value == 0) {
                return;
            }
            // Append the group's aggregate to its builder.
            status = aggregate_builder_casted->Append(aggregate_casted->value);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        case arrow::Type::DOUBLE: {
            // Downcast the aggregate builder
            auto aggregate_builder_casted =
                std::static_pointer_cast<arrow::DoubleBuilder>
                    (aggregate_builder_);
            // Downcast the group aggregate
            auto aggregate_casted =
                std::static_pointer_cast<arrow::DoubleScalar>
                    (aggregate.scalar());
            // Append the group's aggregate to its builder.
            status = aggregate_builder_casted->Append(aggregate_casted->value);
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
        default: {
            std::cerr << "Aggregate does not support aggregations of "
                         "type " + aggregate.type()->ToString() <<
                      std::endl;
        }
    }
}


std::shared_ptr<arrow::Array> Aggregate::get_unique_values(
    ColumnReference group_ref) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;

    std::shared_ptr<arrow::Array> unique_values;

    auto group_by_col = prev_result_->get_table(group_ref.table)
        .get_column_by_name(group_ref.col_name);

    // Fetch the unique values of group_by_col
    status = arrow::compute::Unique(
        &function_context, group_by_col, &unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);

    return unique_values;
}

void Aggregate::sort() {

    arrow::Status status;

    std::shared_ptr<arrow::StructArray> groups;
    std::shared_ptr<arrow::Array> aggregates;

    status = group_builder_->Finish(&groups);
    evaluate_status(status, __FUNCTION__, __LINE__);

    status = aggregate_builder_->Finish(&aggregates);
    evaluate_status(status, __FUNCTION__, __LINE__);

    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;

    for (int i = 0; i < group_by_refs_.size(); i++) {

        // Fetch one of the group columns
        auto values = groups->field(
            out_schema_->GetFieldIndex(order_by_refs_[i].col_name));

        // Sort the groups and aggregates by the order by columns (from left to right)
        for (auto &order_by_ref : order_by_refs_) {
            if (order_by_ref.table == group_by_refs_[i].table &&
                order_by_ref.col_name == group_by_refs_[i].col_name) {

                std::shared_ptr<arrow::Array> sorted_indices;

                status = arrow::compute::SortToIndices(
                    &function_context, *values, &sorted_indices);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Sort group
                status = arrow::compute::Take(
                    &function_context, *values, *sorted_indices, take_options,
                    &values);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Sort aggregates. Note that this may be sorted multiple times if
                // there are multiple orderbys.
                status = arrow::compute::Take(
                    &function_context, *aggregates, *sorted_indices,
                    take_options, &aggregates);
                evaluate_status(status, __FUNCTION__, __LINE__);
            }
        }
        out_table_data_.push_back(values->data());
    }
    out_table_data_.push_back(aggregates->data());
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_unique_value_filter
    (ColumnReference group_ref, arrow::compute::Datum value) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(
        arrow::compute::CompareOperator::EQUAL);
    arrow::compute::Datum out_filter;
    arrow::ArrayVector filter_vector;

    auto group_by_col = prev_result_->get_table(group_ref.table)
        .get_column_by_name(group_ref.col_name);

    for (int i = 0; i < group_by_col->num_chunks(); i++) {

        auto block_col = group_by_col->chunk(i);
        status = arrow::compute::Compare(
            &function_context, block_col, value, compare_options, &out_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        filter_vector.push_back(out_filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}


void Aggregate::finish() {

    out_table_->insert_records(out_table_data_);

    output_result_->append(out_table_);
}

arrow::compute::Datum Aggregate::compute_aggregate(
    AggregateKernels kernel,
    std::shared_ptr<arrow::ChunkedArray> aggregate_col,
    std::shared_ptr<arrow::ChunkedArray> group_filter) {

    arrow::Status status;

    arrow::compute::FilterOptions filter_options;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::Datum datum_col;

    // Apply group filter. Note that the selection filter was already applied in
    // the main loop
    if (group_filter != nullptr) {
        status = arrow::compute::Filter(
            &function_context, aggregate_col, group_filter, filter_options,
            &datum_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
        aggregate_col = datum_col.chunked_array();
    }

    arrow::compute::Datum out_aggregate;

    switch (kernel) {
        case SUM: {
            status = arrow::compute::Sum(
                &function_context, aggregate_col, &out_aggregate);
            break;
        }
            // Returns a Datum of the same type as the column
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
            break;
        }
            // Note that Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(
                &function_context, aggregate_col, &out_aggregate
            );
            break;
        }
    }
    evaluate_status(status, __FUNCTION__, __LINE__);
    return out_aggregate;
}

void Aggregate::compute_aggregates(Task *ctx) {
    //TODO(nicholas): For now, we only perform one aggregate.
    auto table = aggregate_refs_[0].col_ref.table;
    auto aggregate_table = prev_result_->get_table(table);
    auto col_name = aggregate_refs_[0].col_ref.col_name;
    auto aggregate_col = prev_result_->
        get_table(table).get_column_by_name(col_name);

    // Fetch unique values for all Group By columns. You need to do fetch
    // all of them at once, since you need to know how many unique values
    // are in each column to initialize the loop variables.
    for (auto &col_ref : group_by_refs_) {
        all_unique_values_.push_back(get_unique_values(col_ref));
    }

    // DYNAMIC DEPTH NESTED FOR LOOP
    // Initialize the slots to hold the current iteration value for each depth
    int n = groupt_type_->num_children();
    int maxes[n];
    std::vector<int> group_id(n);

    // initialize group_id = {0, 0, ..., 0} and initialize maxes[i] to the number
    // of unique values in group by column i.
    for (int i = 0; i < n; i++) {
        group_id[i] = 0;
        maxes[i] = all_unique_values_[i]->length();
    }

    int index = n - 1; // loop index
    bool exit = false;
    while (!exit) {

        // LOOP BODY START
        // Each task computes the aggregate of one group.
        ctx->spawnLambdaTask(
            [this, group_id, aggregate_col] {
                auto group_filter = get_group_filter(group_id);
                auto aggregate = compute_aggregate(aggregate_refs_[0].kernel,
                                                   aggregate_col, group_filter);
                // Acquire builder_mutex_ so that groups are correctly associated with
                // thier corresponding aggregates.
                std::unique_lock<std::mutex> lock(builder_mutex_);
                insert_group_aggregate(aggregate);
                insert_group(group_id);
            });
        // LOOP BODY END

        if (n == 0)
            break; // Only execute the loop once if there are no group bys

        // INCREMENTED NESTED LOOP
        group_id[n - 1]++;
        while (group_id[index] == maxes[index]) {
            // if n == 0, we have no Group By clause and should exit after one
            // iteration.
            if (index == 0) {
                exit = true;
                break;
            }
            group_id[index--] = 0;
            group_id[index]++;
        }
        index = n - 1;
    }
}

void Aggregate::execute(Task *ctx) {

    ctx->spawnTask(CreateTaskChain(
        // Compute all aggregates
        CreateLambdaTask([this](Task *internal) {
            compute_aggregates(internal);
        }),
        CreateLambdaTask([this](Task *internal) {
            // Sort the output table. Sorting must be done only after all aggregates
            // are computed.
            sort();
        }),
        CreateLambdaTask([this](Task *internal) {
            // Create the output result
            finish();
        })
    ));
}
} // namespace hustle