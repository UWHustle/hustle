
#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>
#include <utils/arrow_compute_wrappers.h>


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

    group_by_refs_ = group_refs;
    order_by_refs_ = order_by_refs;

}


std::vector<std::shared_ptr<arrow::ArrayBuilder>>
Aggregate::get_group_builders() {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
    for (auto &field : group_type_->children()) {
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
    const std::string& agg_col_name) {

    arrow::Status status;
    arrow::SchemaBuilder schema_builder;

    if (group_type_ != nullptr) {
        status = schema_builder.AddFields(group_type_->children());
        evaluate_status(status, __FUNCTION__, __LINE__);
    }
    switch (kernel) {
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

arrow::Datum
Aggregate::get_group_filter(std::vector<int> group_id) {

    arrow::Status status;

    // No Group By clause
    if (group_type_->num_children() == 0) {
        return arrow::Datum();
    }

    arrow::Datum value;
    std::shared_ptr<arrow::ChunkedArray> prev_filter;

    // TODO(nicholas): spawn a new task for each group by column
    // e.g. group_id = [4, 1, 2]
    // We get the filter for all_unique_values[0][4], all_unique_values[1][1],
    // and all_unique_values[3][2]. Recall that all_unique_values[i] is an array
    // of all the unique values of the ith GROUP BY column
    for (int field_i = 0; field_i < group_type_->num_children(); field_i++) {

        std::shared_ptr<arrow::ChunkedArray> next_filter;

        switch (group_type_->child(field_i)->type()->id()) {
            case arrow::Type::STRING: {
                // Downcast an Array of unique values.
                auto one_unique_value_casted =
                    std::static_pointer_cast<arrow::StringArray>
                        (all_unique_values_[field_i]);
                // Fetch a particular unique value from the array specified by
                // the group_id
                value = arrow::Datum(
                    std::make_shared<arrow::StringScalar>(
                        one_unique_value_casted->GetString(
                            group_id[field_i])));
                // Get the filter for this particular unique value.
                next_filter = get_unique_value_filter(group_by_refs_[field_i],
                                                      value);
                break;
            }
            case arrow::Type::INT64: {
                // Downcast an Array of unique values.
                auto one_unique_value_casted =
                    std::static_pointer_cast<arrow::Int64Array>
                        (all_unique_values_[field_i]);
                // Fetch a particular unique value from the array specified by
                // the group_id
                value = arrow::Datum(
                    std::make_shared<arrow::Int64Scalar>(
                        one_unique_value_casted->Value(group_id[field_i])));
                // Get the filter for this particular unique value.
                next_filter = get_unique_value_filter(group_by_refs_[field_i],
                                                      value);
                break;
            }
            default: {
                std::cerr << "invalid type" << std::endl;
            }
        }

        arrow::Datum temp_filter;
        arrow::ArrayVector filter_vector;

        // Perform a logical AND on all the unique value filters
        if (prev_filter != nullptr) {
            for (int j = 0; j < prev_filter->num_chunks(); j++) {
                status = arrow::compute::And(prev_filter->chunk(j),
                                             next_filter->chunk(j)).Value(&temp_filter);
                evaluate_status(status, __FUNCTION__, __LINE__);

                filter_vector.push_back(temp_filter.make_array());
            }

            prev_filter = std::make_shared<arrow::ChunkedArray>(filter_vector);
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
    for (int i = 0; i < group_type_->num_children(); i++) {
        switch (group_type_->child(i)->type()->id()) {

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
                             + group_type_->child(i)->type()->ToString()
                          << std::endl;
            }
        }
    }
    // StructBuilder does not automatically update its length when we append to
    // its children. We must do this manually.
    status = group_builder_->Append(true);
    evaluate_status(status, __FUNCTION__, __LINE__);
}

void Aggregate::insert_group_aggregate(const arrow::Datum& aggregate) {

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


arrow::Datum Aggregate::get_unique_values(
    const ColumnReference& group_ref) {

    arrow::Status status;
    auto group_by_col = group_by_cols_[group_ref.col_name];

    // Get the unique values in group_by_col
    arrow::Datum unique_values;
    status = arrow::compute::Unique(group_by_col).Value(&unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);


    return unique_values;
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_unique_value_filter
    (const ColumnReference& group_ref, arrow::Datum value) {

    arrow::Status status;
    arrow::Datum out_filter;
    arrow::ArrayVector filter_vector;
    arrow::compute::CompareOptions compare_options(arrow::compute::EQUAL);

    auto group_by_col = group_by_cols_[group_ref.col_name];

    // TODO(nicholas): spawn a new task for each block
    for (int i = 0; i < group_by_col->num_chunks(); i++) {

        auto block_col = group_by_col->chunk(i);
        status = arrow::compute::Compare(block_col, value, compare_options).Value(&out_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        filter_vector.push_back(out_filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}


void Aggregate::finish() {

    arrow::Status status;

    // Create Arrow Arrays from the ArrayBuilders
    std::shared_ptr<arrow::StructArray> groups_temp;
    status = group_builder_->Finish(&groups_temp);
    evaluate_status(status, __FUNCTION__, __LINE__);

    for (int i = 0; i < groups_temp->num_fields(); i++) {
        groups_.push_back(groups_temp->field(i));
    }

    std::shared_ptr<arrow::Array> agg_temp;
    status = aggregate_builder_->Finish(&agg_temp);
    evaluate_status(status, __FUNCTION__, __LINE__);
    aggregates_.value = agg_temp->data();

    // Sort the output according to the ORDER BY clause
    sort();

    // Add the sorted data to the output table, and wrap the output table in
    // the output OperatorResult.
    for (auto &group_values : groups_) {
        output_table_data_.push_back(group_values.make_array()->data());
    }
    output_table_data_.push_back(aggregates_.make_array()->data());

    output_table_->insert_records(output_table_data_);
    output_result_->append(output_table_);
}

arrow::Datum Aggregate::compute_aggregate(
    AggregateKernels kernel,
    const arrow::Datum& aggregate_col) {

    arrow::Status status;
    arrow::Datum out_aggregate;

    switch (kernel) {
        case SUM: {
            status = arrow::compute::Sum(aggregate_col).Value(&out_aggregate);
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
            break;
        }
            // Note that Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(aggregate_col).Value(&out_aggregate);
            break;
        }
    }
    evaluate_status(status, __FUNCTION__, __LINE__);
    return out_aggregate;
}



void Aggregate::initialize() {

    // Fetch the fields associated with each groupby column.
    std::vector<std::shared_ptr<arrow::Field>> group_by_fields;
    for (auto &group_by : group_by_refs_) {
        group_by_fields.push_back(
            group_by.table->get_schema()
                ->GetFieldByName(group_by.col_name));
    }

    sort_aggregate_col_ = false;
    for (auto &order_by : order_by_refs_) {
        if (order_by.table == nullptr) {
            sort_aggregate_col_ = true;
        }
    }

    // Initialize a StructBuilder containing one builder for each group
    // by column.
    group_type_ = arrow::struct_(group_by_fields);
    group_builder_ = std::make_shared<arrow::StructBuilder>(
        group_type_, arrow::default_memory_pool(), get_group_builders());

    // Initialize aggregate builder
    aggregate_builder_ = get_aggregate_builder(aggregate_refs_[0].kernel);

    // Initialize output table schema. group_type_ must be initialized beforehand.
    out_schema_ = get_output_schema(aggregate_refs_[0].kernel,
                                    aggregate_refs_[0].agg_name);
    //Initialize output table.
    output_table_ = std::make_shared<Table>("aggregate", out_schema_, BLOCK_SIZE);

    // Fetch unique values for all Group By columns.
    for (auto &col_ref : group_by_refs_) {
        group_by_cols_.emplace(col_ref.col_name, prev_result_->get_table(col_ref.table).get_column_by_name(col_ref.col_name));
        all_unique_values_.push_back(get_unique_values(col_ref).make_array());
    }
}

void Aggregate::compute_group_aggregate(
    const std::vector<int>& group_id,
    arrow::Datum agg_col) {

    arrow::Status status;
    auto group_filter = get_group_filter(group_id);

    // Apply group filter to the aggregate column
    if (group_filter.kind() != arrow::Datum::NONE) {
        status = arrow::compute::Filter(agg_col, group_filter).Value(&agg_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }

    if (agg_col.length() > 0) {
        // Compute the aggregate over the filtered agg_col
        auto aggregate = compute_aggregate(aggregate_refs_[0].kernel, agg_col);

        // Acquire builder_mutex_ so that groups are correctly associated with
        // their corresponding aggregates
        std::unique_lock<std::mutex> lock(builder_mutex_);
        insert_group_aggregate(aggregate);
        insert_group(group_id);
    }
}

void Aggregate::compute_aggregates(Task *ctx) {

    //TODO(nicholas): For now, we only perform one aggregate.
    auto table = aggregate_refs_[0].col_ref.table;
    auto col_name = aggregate_refs_[0].col_ref.col_name;
    auto agg_lazy_table = prev_result_->get_table(table);
    auto agg_col = agg_lazy_table.get_column_by_name(col_name);

    // Initialize the slots to hold the current iteration value for each depth
    int n = group_type_->num_children();
    int maxes[n];
    std::vector<int> group_id(n);

    // DYNAMIC DEPTH NESTED LOOP:
    // We must handle an arbitrary number of GROUP BY columns. We need a dynamic
    // nested loop to iterate over all possible groups. If maxes = {2, 3}
    // (i.e. we have two group by columns which have 2 and 3 unique values,
    // respectively), then group_id takes on the following values at each iteration:
    //
    // {0, 0}
    // {0, 1}
    // {0, 2}
    // {1, 0}
    // {1, 1}
    // {1, 2}

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
        // Task = compute the aggregate of one group.
        ctx->spawnLambdaTask(
            [this, group_id, agg_col] {
                compute_group_aggregate(group_id, agg_col);
            });
        // LOOP BODY END

        if (n == 0)
            break; // Only execute the loop once if there are no group bys

        // INCREMENT group_id
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
        // Task 1 = Compute all aggregates
        CreateLambdaTask([this](Task *internal) {
            initialize();
            compute_aggregates(internal);
        }),
        // Task 2 = Create the output result
        CreateLambdaTask([this](Task *internal) {
            finish();
        })
    ));
}

void Aggregate::sort() {

    // The columns in the GROUP BY and ORDER BY clause may not directly correspond
    // to the same column, e.g we may have
    // GROUP BY R.a, R.b
    // ORDER BY R.b, R.a
    // order_by_group[i] is the index at which the ith ORDER BY column appears in
    // the GROUP BY clause. In this example, order_to_group = {1, 0}
    std::vector<int> order_to_group;

    for (auto & order_by_ref : order_by_refs_) {
        for (int j=0; j<group_by_refs_.size(); j++) {
            if (order_by_ref.table == group_by_refs_[j].table) {
                order_to_group.push_back(j);
            }
        }
    }

    arrow::Datum sorted_indices;
    arrow::Status status;
    // If we are sorting after computing all aggregates, we evaluate the ORDER BY
    // clause in reverse order.
    for (int i=order_by_refs_.size()-1; i>=0; i--) {

        auto order_ref = order_by_refs_[i];

        // A nullptr indicates that we are sorting by the aggregate column
        // TODO(nicholas): better way to indicate we want to sort the aggregate?
        if (order_ref.table == nullptr) {
            status = arrow::compute::SortToIndices(*aggregates_.make_array()).Value(&sorted_indices);
            evaluate_status(status, __FUNCTION__, __LINE__);
        } else {
            auto group = groups_[order_to_group[i]];
            status = arrow::compute::SortToIndices(*group.make_array()).Value(&sorted_indices);
            evaluate_status(status, __FUNCTION__, __LINE__);

        }
        status = arrow::compute::Take(aggregates_, sorted_indices).Value(&aggregates_);
        evaluate_status(status, __FUNCTION__, __LINE__);


        for (auto &group: groups_) {
            status = arrow::compute::Take(group, sorted_indices).Value(&group);
            evaluate_status(status, __FUNCTION__, __LINE__);

        }
    }
}
} // namespace hustle