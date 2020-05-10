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
            std::vector<AggregateReference> aggregate_refs,
            std::vector<ColumnReference> group_refs,
            std::vector<ColumnReference> order_by_refs) :
            Operator(query_id) {

        prev_result_ = prev_result;
        aggregate_refs_ = aggregate_refs;

        group_by_refs_ = std::move(group_refs);
        order_by_refs_ = std::move(order_by_refs);

        aggregate_builder_ = get_aggregate_builder(aggregate_refs_[0].kernel);

        // Fetch the fields associated with each groupby column.
        std::vector<std::shared_ptr<arrow::Field>> group_by_fields;
        for(auto &group_by : group_by_refs_) {
            group_by_fields.push_back(
                    group_by.table->get_schema()
                    ->GetFieldByName(group_by.col_name));
        }

        // Initialize a StructBuilder containing one builder for each group
        // by column.
        group_type = arrow::struct_(group_by_fields);
        group_builder = std::make_shared<arrow::StructBuilder>(
                group_type, arrow::default_memory_pool(), get_group_builders());

        // Fetch unique values for all Group By columns. You need to do fetch
        // all of them at once, since you need to know how many unique values
        // are in each column to initialize the loop variables.
        for (auto &col_ref : group_by_refs_) {
            unique_values_.push_back(get_unique_values(col_ref));
        }

    }


    std::vector<std::shared_ptr<arrow::ArrayBuilder>>
    Aggregate::get_group_builders() {

        arrow::Status status;
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
        for (auto &field : group_type->children()) {
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
                default: {
                    std::cerr << "Aggregate does not support group bys of type "
                    + field->type()->ToString() << std::endl;
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

    std::shared_ptr<arrow::Schema> Aggregate::get_output_schema(
            AggregateKernels kernel,
            std::string agg_col_name) {

        arrow::Status status;
        arrow::SchemaBuilder schema_builder;

        status = schema_builder.AddFields(group_type->children());
        evaluate_status(status, __FUNCTION__, __LINE__);

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

    std::shared_ptr<arrow::ChunkedArray> Aggregate::get_group_filter(int* its) {

        arrow::Status status;
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());

        // No Group By clause
        if (group_type->num_children() == 0) {
            return nullptr;
        }

        arrow::compute::Datum value;
        std::shared_ptr<arrow::ChunkedArray> prev_filter;

        // Fetch the next Group By filter and AND it with the previous filter
        for (int field_i=0; field_i<group_type->num_children(); field_i++) {

            std::shared_ptr<arrow::ChunkedArray> next_filter;

            switch(group_type->child(field_i)->type()->id()) {
                case arrow::Type::STRING: {
                    auto one_unique_values_casted =
                            std::static_pointer_cast<arrow::StringArray>
                                    (unique_values_[field_i]);
                    value = arrow::compute::Datum(
                            std::make_shared<arrow::StringScalar>(
                                    one_unique_values_casted->GetString(its[field_i])));
                    next_filter = get_filter(group_by_refs_[field_i], value);
                    break;
                }
                case arrow::Type::INT64: {
                    auto one_unique_values_casted =
                            std::static_pointer_cast<arrow::Int64Array>
                                    (unique_values_[field_i]);
                    value = arrow::compute::Datum(
                            std::make_shared<arrow::Int64Scalar>(
                                    one_unique_values_casted->Value(its[field_i])));
                    next_filter = get_filter(group_by_refs_[field_i], value);
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
            }
            else {
                prev_filter = next_filter;
            }
        }

        return prev_filter;
    }

    void Aggregate::insert_group(int *its) {

        arrow::Status status;
        for (int i=0; i<group_type->num_children(); i++) {
            switch(group_type->child(i)->type()->id()) {
                case arrow::Type::STRING: {
                    auto one_group_builder = (arrow::StringBuilder*)
                            (group_builder->child(i));
                    auto one_unique_values =
                            std::static_pointer_cast<arrow::StringArray>
                                    (unique_values_[i]);
                    status = one_group_builder->Append(one_unique_values->GetString
                            (its[i]));
                    evaluate_status(status, __FUNCTION__, __LINE__);
                    break;
                }
                case arrow::Type::INT64: {
                    auto one_group_builder = (arrow::Int64Builder *)
                            (group_builder->child(i));
                    auto one_unique_values =
                            std::static_pointer_cast<arrow::Int64Array>
                                    (unique_values_[i]);
                    status = one_group_builder->Append
                            (one_unique_values->Value(its[i]));
                    evaluate_status(status, __FUNCTION__, __LINE__);
                    break;
                }
                default: {
                    std::cerr << "Cannot insert unsupported aggregate type: "
                    + group_type->child(i)->type()->ToString() << std::endl;
                }
            }
        }

        status = group_builder->Append(true);
        evaluate_status(status, __FUNCTION__, __LINE__);
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
        arrow::compute::FilterOptions filter_options;
        std::shared_ptr<arrow::Array> unique_values;

        auto group_by_col = prev_result_->get_table(group_ref.table)
                .get_column_by_name(group_ref.col_name);

        status = arrow::compute::Unique(&function_context, group_by_col,
                                        &unique_values);
        evaluate_status(status, __FUNCTION__, __LINE__);

        // If this field is in the Order By clause, sort it now. This assumes
        // that column names are unique within a table, which is a fair
        // assumption to make.
        for (auto & order_ref : order_by_refs_) {
            if (order_ref.table == group_ref.table &&
                order_ref.col_name == group_ref.col_name ) {

                std::shared_ptr<arrow::Array> sorted_indices;

                status = arrow::compute::SortToIndices(&function_context,
                                                       *unique_values, &sorted_indices);
                evaluate_status(status, __FUNCTION__, __LINE__);

                status = arrow::compute::Take(&function_context, *unique_values,
                                              *sorted_indices, take_options, &unique_values);
                evaluate_status(status, __FUNCTION__, __LINE__);
            }
        }

        return unique_values;
    }

    std::shared_ptr<arrow::ChunkedArray> Aggregate::get_filter
            (ColumnReference group_ref, arrow::compute::Datum value) {

        arrow::Status status;

        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        arrow::compute::CompareOptions compare_options(
                arrow::compute::CompareOperator::EQUAL);
        arrow::compute::TakeOptions take_options;
        arrow::compute::FilterOptions filter_options;
        arrow::compute::Datum out_filter;
        arrow::ArrayVector filter_vector;

        auto group_lazy_table = prev_result_->get_table(group_ref.table);
        auto filter = group_lazy_table.filter;
        auto selection = group_lazy_table.indices;

        auto group_by_col = prev_result_->get_table(group_ref.table)
                .get_column_by_name(group_ref.col_name);

        for (int i=0; i<group_by_col->num_chunks(); i++) {
            auto block_col = group_by_col->chunk(i);

            // Note that Compare cannot operate on ChunkedArrays
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

    std::shared_ptr<OperatorResult> Aggregate::run() {
        auto out = std::make_shared<OperatorResult>();

        arrow::Status status;

        auto out_schema = get_output_schema(aggregate_refs_[0].kernel,
                aggregate_refs_[0].agg_name);
        auto out_table = std::make_shared<Table>("aggregate", out_schema,
                                                 BLOCK_SIZE);

        //TODO(nicholas): For now, we only perform one aggregate.
        auto table = aggregate_refs_[0].col_ref.table;
        auto aggregate_table = prev_result_->get_table(table);
        auto col_name = aggregate_refs_[0].col_ref.col_name;

        auto aggregate_col = prev_result_->get_table(table).get_column_by_name
                (col_name);

        // DYNAMIC DEPTH NESTED FOR LOOP
        // Initialize the slots to hold the current iteration value for each depth
        int n = group_type->num_children();
        int its[n];
        int maxes[n];

        for (int i = 0; i < n; i++) {
            its[i] = 0;
            maxes[i] = unique_values_[i]->length();
        }

        int index = n - 1;
        bool exit = false;
        while (!exit){

            // LOOP BODY START
            std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;

            auto group_filter = get_group_filter(its);
            auto aggregate = compute_aggregate(aggregate_refs_[0].kernel,
                                               aggregate_col, group_filter);
            insert_group_aggregate(aggregate);
            insert_group(its);
            // LOOP BODY END

            if (n == 0) break; // edge case

            // INCREMENTED NESTED LOOP
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

        // Build the output table
        std::vector<std::shared_ptr<arrow::ArrayData>> table_data;
        for (int i=0; i<group_by_refs_.size(); i++) {
            table_data.push_back(groups->field(i)->data());
        }
        table_data.push_back(aggregates->data());
        out_table->insert_records(table_data);

//        for (auto aggregate_ref : aggregate_refs_)
        out->append(out_table);
        return out;
    }
} // namespace hustle
