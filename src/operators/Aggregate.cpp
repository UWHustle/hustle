
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

arrow::compute::Datum
Aggregate::get_group_filter(std::vector<int> group_id) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());

    // No Group By clause
    if (group_type_->num_children() == 0) {
        return arrow::compute::Datum();
    }

    arrow::compute::Datum value;
    std::shared_ptr<arrow::ChunkedArray> prev_filter;

    // TODO(nicholas): spawn a new task for each group by column
    // Fetch the next Group By filter and AND it with the previous filter
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
                value = arrow::compute::Datum(
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
                value = arrow::compute::Datum(
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

bool Aggregate::insert_group_aggregate(const arrow::compute::Datum& aggregate, int agg_index) {

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
                tuple_ordering_[agg_index] = -1;
                return false;
            }
            // Append the group's aggregate to its builder.
            status = aggregate_builder_casted->Append(aggregate_casted->value);
            tuple_ordering_[agg_index] = aggregate_builder_casted->length()-1;
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
    return true;
}


arrow::compute::Datum Aggregate::get_unique_values(
    const ColumnReference& group_ref) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> unique_values;

//    auto group_by_col = prev_result_->get_table(group_ref.table)
//        .get_column_by_name(group_ref.col_name);
    auto group_by_col = group_by_cols_[group_ref.col_name];

    status = arrow::compute::Unique(&function_context, group_by_col,
                                    &unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);

    arrow::compute::Datum sorted_unique_values = unique_values;

    // If this field is in the Order By clause, sort it now.
    for (auto & order_ref : order_by_refs_) {
        if (order_ref.table == group_ref.table &&
            order_ref.col_name == group_ref.col_name ) {

            sort_datum(unique_values, &sorted_unique_values);
        }
    }

    return sorted_unique_values.make_array();
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_unique_value_filter
    (const ColumnReference& group_ref, arrow::compute::Datum value) {

    arrow::compute::Datum out_filter;
    arrow::ArrayVector filter_vector;

    auto group_by_col = group_by_cols_[group_ref.col_name];

    // TODO(nicholas): spawn a new task for each block
    for (int i = 0; i < group_by_col->num_chunks(); i++) {

        auto block_col = group_by_col->chunk(i);
        compare(block_col, value, arrow::compute::CompareOperator::EQUAL, &out_filter);

        filter_vector.push_back(out_filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}


void Aggregate::finish() {

    arrow::Status status;

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

    // Prepare to sort output
    arrow::Int64Builder indices_builder;
    std::shared_ptr<arrow::Array> indices;

    for (auto &index : tuple_ordering_) {
        if(index >= 0) {
            status = indices_builder.Append(index);
            evaluate_status(status, __FUNCTION__, __LINE__);
        }
    }
    status = indices_builder.Finish(&indices);
    evaluate_status(status, __FUNCTION__, __LINE__);

    for (auto &group: groups_) {
        apply_indices(group, indices, &group);
    }

    // Sort aggregates
    apply_indices(aggregates_, indices, &aggregates_);

    if (sort_aggregate_col_) {
        sort();
    }

    for (auto &group_values : groups_) {
        out_table_data_.push_back(group_values.make_array()->data());
    }
    out_table_data_.push_back(aggregates_.make_array()->data());


    out_table_->insert_records(out_table_data_);
    output_result_->append(out_table_);
}

arrow::compute::Datum Aggregate::compute_aggregate(
    AggregateKernels kernel,
    const arrow::compute::Datum& aggregate_col) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::Datum out_aggregate;

    switch (kernel) {
        case SUM: {
            status = arrow::compute::Sum(
                &function_context, aggregate_col, &out_aggregate);
            break;
        }
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
    out_table_ = std::make_shared<Table>("aggregate", out_schema_, BLOCK_SIZE);

    // Fetch unique values for all Group By columns.
    for (auto &col_ref : group_by_refs_) {
        group_by_cols_.emplace(col_ref.col_name, prev_result_->get_table(col_ref.table).get_column_by_name(col_ref.col_name));
        all_unique_values_.push_back(get_unique_values(col_ref).make_array());
    }
}

void Aggregate::compute_group_aggregate(
    int agg_index,
    const std::vector<int>& group_id,
    arrow::compute::Datum agg_col) {

    auto group_filter = get_group_filter(group_id);

    arrow::compute::Datum datum_col;

    // Apply group filter
    if (group_filter.kind() != arrow::compute::Datum::NONE) {
        apply_filter(agg_col, group_filter, &agg_col);
    }

    // Compute the aggregate over the filtered agg_col
    auto aggregate = compute_aggregate(aggregate_refs_[0].kernel, agg_col);

    // Acquire builder_mutex_ so that groups are correctly associated with
    // their corresponding aggregates
    std::unique_lock<std::mutex> lock(builder_mutex_);
    auto is_nonzero_agg = insert_group_aggregate(aggregate, agg_index);
    if (is_nonzero_agg) insert_group(group_id);
}

void Aggregate::compute_aggregates(Task *ctx) {
    arrow::Status status;
    //TODO(nicholas): For now, we only perform one aggregate.
    auto table = aggregate_refs_[0].col_ref.table;
    auto col_name = aggregate_refs_[0].col_ref.col_name;
    auto agg_lazy_table = prev_result_->get_table(table);
    auto agg_col = agg_lazy_table.get_column_by_name(col_name);

    // DYNAMIC DEPTH NESTED FOR LOOP
    // Initialize the slots to hold the current iteration value for each depth
    int n = group_type_->num_children();
    int maxes[n];
    std::vector<int> group_id(n);

    // Number of aggregates to be computed. Equal to the product of the number
    // of unique values in each group by column.
    int num_agg = 1;
    // Index of the aggregate we are currently computing
    int agg_index = 0;

    // initialize group_id = {0, 0, ..., 0} and initialize maxes[i] to the number
    // of unique values in group by column i.
    for (int i = 0; i < n; i++) {
        group_id[i] = 0;
        maxes[i] = all_unique_values_[i]->length();
        num_agg *= maxes[i];
    }

    tuple_ordering_.resize(num_agg);
    status = group_builder_->Resize(num_agg);
    evaluate_status(status, __FUNCTION__, __LINE__);

    int index = n - 1; // loop index
    bool exit = false;
    while (!exit) {

        // LOOP BODY START
        // Task = compute the aggregate of one group.
        ctx->spawnLambdaTask(
            [this, group_id, agg_col, agg_index] {
                compute_group_aggregate(agg_index, group_id, agg_col);
            });
        agg_index++;
        // LOOP BODY END

        if (n == 0)
            break; // Only execute the loop once if there are no group bys

        // INCREMENT LOOP VARIABLE
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

    arrow::compute::Datum sorted_indices;

    // If we are sorting after computing all aggregates, we evaluate the ORDER BY
    // clause in reverse order.
    for (int i=order_by_refs_.size()-1; i>=0; i--) {

        auto order_ref = order_by_refs_[i];

        if (order_ref.table == nullptr) {
            sort_to_indices(aggregates_, &sorted_indices);
        } else {
            auto group = groups_[order_to_group[i]];
            sort_to_indices(group, &sorted_indices);
        }
        apply_indices(aggregates_, sorted_indices, &aggregates_);

        for (auto &group: groups_) {
            apply_indices(group, sorted_indices, &group);
        }
    }
}
} // namespace hustle