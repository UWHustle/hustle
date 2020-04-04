#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {

Aggregate::Aggregate(
        std::vector<JoinResult> join_result,
                     std::vector<AggregateUnit> aggregate_units,
                     std::vector<ColumnReference> group_bys,
                     std::vector<ColumnReference> order_bys) {

    join_result_ = join_result;
    aggregate_units_ = aggregate_units;

    group_bys_ = std::move(group_bys);
    order_bys_ = std::move(order_bys);

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

std::shared_ptr<Table> Aggregate::aggregate() {

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
                        std::make_shared<arrow::Int64Builder>());
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
    arrow::compute::FilterOptions filter_options;
    arrow::compute::TakeOptions take_options;

    auto out_schema = get_output_schema(aggregate_units_[0].kernel);
    auto out_table = std::make_shared<Table>("aggregate", out_schema,
                                             BLOCK_SIZE);

    auto table = aggregate_units_[0].table; //TODO(nicholas)
    std::vector<std::shared_ptr<arrow::Array>> unique_values;
    // Fetch unique values for all Group By columns
    for (int i=0; i< group_by_fields_.size(); i++) {
        unique_values.push_back(
                get_unique_values(group_bys_[i]));
//        std::cout << unique_values[i]->ToString() << std::endl;
    }


    // TODO(nicholas): for now, assume that the selections are always arrays
    //  of indices, not filters.
    // TODO(nicholas): If want to compute multiple aggregates, we'll need to
    // move this inside of the loop but only call it once.
//    auto table = aggregate_units_[0].table;

    arrow::compute::Datum filter;
    arrow::compute::Datum selection;
    std::string name;

    int aggregate_index = -1;
    for (int i=0; i<join_result_.size(); i++) {
        if (table == join_result_[i].table) {
            aggregate_index = i;
            break;
        }
    }
    filter = join_result_[aggregate_index].filter;
    selection = join_result_[aggregate_index].selection;
    name = aggregate_units_[0].col_name;

    auto aggregate_col = table->get_column_by_name(
            aggregate_units_[0].col_name);

    auto datum_col = arrow::compute::Datum(aggregate_col);
    // Apply filter
    if (filter.kind() ==
        arrow::compute::Datum::CHUNKED_ARRAY) {
        status = arrow::compute::Filter(
                &function_context,
                aggregate_col,
                filter.chunked_array(),
                filter_options,
                &datum_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
        aggregate_col = datum_col.chunked_array();
    }
    // Take indices
    if (selection.kind() ==
        arrow::compute::Datum::ARRAY) {
        status = arrow::compute::Take(
                &function_context,
                *aggregate_col,
                *selection.make_array(),
                take_options,
                &aggregate_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }

//    if (filter.kind() == arrow::compute::Datum::CHUNKED_ARRAY) {
//        status = arrow::compute::Filter(&function_context,
//                                        *aggregate_col,
//                                        *filter.chunked_array(),
//                                        &aggregate_col);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//    }
    //////////


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
//        for (int k=0; k<n; k++) {
//            std::cout << its[k] << " ";
//        }
//        std::cout << std::endl;
        // DoSomething() loop

//        std::cout << filter.length() << std::endl;
//        std::cout << aggregate_col->length() << std::endl;
//
        //////////
        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;

        auto group_filter = get_group_filter(unique_values, its);
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
    //TODO(nicholas): remove redundant call to get_unique_values()
    auto one_unique_values =
            get_unique_values(group_bys_[0]);

    arrow::compute::Datum value;
    std::shared_ptr<arrow::ChunkedArray> filter;

    switch(group_by_fields_[0]->type()->id()) {
        case arrow::Type::STRING: {
            auto one_unique_values_casted =
                    std::static_pointer_cast<arrow::StringArray>(unique_values[0]);
            value = arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>(
                            one_unique_values_casted->GetString(its[0])));
            filter = get_filter(group_bys_[0], group_by_fields_[0], value);
            break;
        }
        case arrow::Type::INT64: {
            auto one_unique_values_casted =
                    std::static_pointer_cast<arrow::Int64Array>
                            (unique_values[0]);
            value = arrow::compute::Datum(
                    std::make_shared<arrow::Int64Scalar>(
                            one_unique_values_casted->Value(its[0])));
            filter = get_filter(group_bys_[0], group_by_fields_[0], value);
            break;
        }
        default: {
            std::cerr << "invalid type" << std::endl;
        }
    }


    // Fetch the next Group By filter and AND it with our current filter
    for (int field_i=1; field_i<group_by_fields_.size(); field_i++) {

        std::shared_ptr<arrow::ChunkedArray> next_filter;

        switch(group_by_fields_[field_i]->type()->id()) {
            case arrow::Type::STRING: {
                auto one_unique_values_casted =
                        std::static_pointer_cast<arrow::StringArray>(unique_values[field_i]);
                value = arrow::compute::Datum(
                        std::make_shared<arrow::StringScalar>(
                                one_unique_values_casted->GetString(its[field_i])));
                next_filter = get_filter(group_bys_[field_i], group_by_fields_[field_i],
                        value);
                break;
            }
            case arrow::Type::INT64: {
                auto one_unique_values_casted =
                        std::static_pointer_cast<arrow::Int64Array>
                                (unique_values[field_i]);
                value = arrow::compute::Datum(
                        std::make_shared<arrow::Int64Scalar>(
                                one_unique_values_casted->Value(its[field_i])));
                next_filter = get_filter(group_bys_[field_i], group_by_fields_[field_i],
                        value);
                break;
            }
            default: {
                std::cerr << "invalid type" << std::endl;
            }
        }

        arrow::compute::Datum temp_filter;
        arrow::ArrayVector filter_vector;

        for (int j = 0; j < filter->num_chunks(); j++) {

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
                break;
            }
            case arrow::Type::INT64: {
                auto one_group_builder = (arrow::Int64Builder *)
                        (group_builder->child(i));
//                one_group_builder->Append()
                auto one_unique_values =
                        std::static_pointer_cast<arrow::Int64Array>
                                (unique_values[i]);
                status = one_group_builder->Append
                        (one_unique_values->Value(its[i]));
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
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
        ColumnReference group_ref) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    arrow::compute::FilterOptions filter_options;
    std::shared_ptr<arrow::Array> unique_values;

    auto group_by_col = group_ref.table->get_column_by_name(group_ref.col_name);

    arrow::compute::Datum filter;
    arrow::compute::Datum selection;
    for (int i=0; i<join_result_.size(); i++) {
        if (join_result_[i].table == group_ref.table) {
            filter = join_result_[i].filter;
            selection = join_result_[i].selection;
        }
    }

//    std::cout << join_result_[0].selection.make_array()->ToString() <<
//    std::endl;

    auto datum_col = arrow::compute::Datum(group_by_col);
    if( filter.kind() == arrow::compute::Datum::CHUNKED_ARRAY) {
        status = arrow::compute::Filter(&function_context,
                                        group_by_col,
                                        filter.chunked_array(),
                                        filter_options,
                                        &datum_col);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        group_by_col = datum_col.chunked_array();
    }

    status = arrow::compute::Take(
            &function_context,
            *group_by_col,
            *selection.make_array(),
            take_options,
            &group_by_col);
    evaluate_status(status, __FUNCTION__, __LINE__);

    status = arrow::compute::Unique(&function_context, group_by_col,
                                    &unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);

    // If this field is in the Order By clause, sort it now.
    //TODO(nicholas): Unexpected behavior when two group bys have the same
    // column name.
    for (auto & order_ref : order_bys_) {
        if (order_ref.table == group_ref.table &&
            order_ref.col_name == group_ref.col_name ) {

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
(ColumnReference group_ref,
        std::shared_ptr<arrow::Field> field, arrow::compute::Datum value) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(
            arrow::compute::CompareOperator::EQUAL);
    arrow::compute::TakeOptions take_options;
    arrow::compute::FilterOptions filter_options;
    arrow::compute::Datum out_filter;
    arrow::ArrayVector filter_vector;

    arrow::compute::Datum filter;
    arrow::compute::Datum selection;
    for (int i=0; i<join_result_.size(); i++) {
        if (join_result_[i].table == group_ref.table) {
            filter = join_result_[i].filter;
            selection = join_result_[i].selection;
        }
    }

    std::shared_ptr<arrow::ChunkedArray> group_by_col = group_ref
            .table->get_column_by_name(group_ref.col_name);

    arrow::compute::Datum datum_col;
    if( filter.kind() == arrow::compute::Datum::CHUNKED_ARRAY) {
        status = arrow::compute::Filter(&function_context,
                                        group_by_col,
                                        filter.chunked_array(),
                                        filter_options,
                                        &datum_col);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        group_by_col = datum_col.chunked_array();
    }

    status = arrow::compute::Take(
            &function_context,
            *group_by_col,
            *selection.make_array(),
            take_options,
            &group_by_col);

    evaluate_status(status, __FUNCTION__, __LINE__);

    for (int i=0; i<group_by_col->num_chunks(); i++) {
        auto block_col = group_by_col->chunk(i);

        // Note that Compare does not operate on ChunkedArrays, so we must
        // compute the filter block by block and combine them into a
        // ChunkedArray.
        status = arrow::compute::Compare(&function_context, block_col, value,
                compare_options, &out_filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        filter_vector.push_back(out_filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
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
    // Note that the selection filter was already applied in the main loop
    // Apply group filter
    if (group_filter != nullptr) {
        status = arrow::compute::Filter(
                &function_context,
                aggregate_col,
                group_filter,
                filter_options,
                &datum_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
        aggregate_col = datum_col.chunked_array();
    }

    arrow::compute::Datum out_aggregate;

    switch (kernel) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = arrow::compute::Sum(
                    &function_context,
                    aggregate_col,
                    &out_aggregate
            );
            break;
        }
            // Returns a Datum of the same type as the column
        case COUNT: {
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
                    aggregate_col,
                    &out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
            // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(
                    &function_context,
                    aggregate_col,
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
