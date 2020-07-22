
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

    prev_result_ = std::move(prev_result);
    output_result_ = std::move(output_result);
    aggregate_refs_ = std::move(aggregate_refs);

    group_by_refs_ = std::move(group_refs);
    order_by_refs_ = std::move(order_by_refs);

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
            case arrow::Type::FIXED_SIZE_BINARY: {
                group_builders.push_back(
                    std::make_shared<arrow::FixedSizeBinaryBuilder>(field->type()));
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

void Aggregate::scan_block(std::vector<const uint32_t *>& group_map, int chunk_i, std::vector<arrow::ArrayVector>& filter_vectors) {

    auto chunk_length = agg_col_.chunked_array()->chunk(chunk_i)->length();
    auto num_children = group_by_refs_.size();
    auto agg_col_chunk_data = agg_col_data_[chunk_i];

    for (int field_i = 0; field_i < num_children; ++field_i) {
        group_map[field_i] = uniq_val_maps_[field_i].chunked_array()->chunk(chunk_i)->data()->GetValues<uint32_t>(1, 0);
    }

    for (int row_j = 0; row_j < chunk_length; ++row_j) {

        auto key = 0;
        for (int group_k = 0; group_k < num_children; ++group_k) {
            // @TODO(nicholas): save time by precomputing 10^(k+1)
            key += group_map[group_k][row_j]*pow(10, group_k);
        }

        auto agg_index = group_id_to_agg_index_map_[key];
        aggregates_vec_[agg_index] += agg_col_chunk_data[row_j];
    }
}

void Aggregate::initialize_group_filters(Task* ctx) {
    if (group_type_->num_children() == 0) {
        return;
    }

    ctx->spawnLambdaTask([this](Task* internal) {
        auto num_children = group_by_cols_.size();

        std::vector<arrow::Type::type> field_types;
        std::vector<const uint32_t*> uniq_index_maps;

        for (int field_i = 0; field_i < num_children; field_i++) {
            auto data = all_unique_values_[field_i]->data();
            field_types.push_back(data->type->id());
        }

        auto agg_col = agg_col_.chunked_array();
        auto num_chunks = agg_col->num_chunks();
        agg_col_data_.resize(num_chunks);
        for (int i=0; i<num_chunks; ++i) {
            agg_col_data_[i] = agg_col->chunk(i)->data()->GetValues<int64_t>(1, 0);
        }


        int batch_size = num_chunks / std::thread::hardware_concurrency() /2;
        if (batch_size == 0) batch_size = num_chunks;
        int num_batches = num_chunks / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
        if (num_batches == 0) num_batches = 1;

        for (int batch_i = 0; batch_i < num_batches; batch_i++) {
            // Each task gets the filter for one block and stores it in filter_vector
            internal->spawnLambdaTask([this, num_chunks, num_children, batch_i, batch_size](Task *internal) {
            std::vector<const uint32_t *> group_map;
            group_map.resize(num_children);

                int base_i = batch_i * batch_size;
                for (int i = base_i; i < base_i + batch_size && i < num_chunks; i++) {
                    scan_block(group_map, i, filter_vectors);
                }
            });
        }
    });
}

void Aggregate::insert_group(std::vector<int> group_id, int agg_index) {

    arrow::Status status;
    // Loop over columns in group builder, and append one of its unique values to
    // its builder.
    for (int i = 0; i < group_type_->num_children(); ++i) {
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

void Aggregate::insert_group_aggregate(const arrow::Datum& aggregate, int agg_index) {

    arrow::Status status;

    auto aggregate_builder_casted =
        std::static_pointer_cast<arrow::Int64Builder>
            (aggregate_builder_);
    status = aggregate_builder_casted->Append(aggregates_vec_[agg_index]);
    evaluate_status(status, __FUNCTION__, __LINE__);
    return;
    // Append a group's aggregate to its builder.
    switch (aggregate.type()->id()) {
        case arrow::Type::INT64: {
            // Downcast the aggregate builder

//            // Downcast the group aggregate
//            auto aggregate_casted =
//                std::static_pointer_cast<arrow::Int64Scalar>
//                    (aggregate.scalar());
            // Append the group's aggregate to its builder.

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
    auto group_by_col = group_by_cols_[group_by_col_names_to_index_[group_ref.col_name]];

    // Get the unique values in group_by_col
    arrow::Datum unique_values;
    status = arrow::compute::Unique(group_by_col).Value(&unique_values);

    evaluate_status(status, __FUNCTION__, __LINE__);


    return unique_values;
}

void Aggregate::finish() {

    arrow::Status status;

    // Create Arrow Arrays from the ArrayBuilders
    std::shared_ptr<arrow::StructArray> groups_temp;
    status = group_builder_->Finish(&groups_temp);
    evaluate_status(status, __FUNCTION__, __LINE__);

    for (int i = 0; i < groups_temp->num_fields(); ++i) {
        groups_.emplace_back(groups_temp->field(i));
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

void Aggregate::initialize_variables(Task* ctx) {
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

    all_unique_values_.resize(group_by_refs_.size());
    unique_value_filters_.resize(group_by_refs_.size());
    group_by_cols_.resize(group_by_refs_.size());

    for (auto &group_by : group_by_refs_) {
        group_by_tables_.push_back(prev_result_->get_table(group_by.table));
    }

    auto num_children = group_by_refs_.size();
    uniq_val_maps_.resize(num_children);
}

void Aggregate::initialize_group_by_column(Task* ctx, int i) {
    std::scoped_lock<std::mutex> lock(mutex_);
    group_by_col_names_to_index_.emplace(group_by_refs_[i].col_name, i);
    group_by_tables_[i].get_column_by_name(ctx, group_by_refs_[i].col_name, group_by_cols_[i]);
}

void Aggregate::initialize(Task* ctx) {

    std::vector<std::shared_ptr<Context>> contexts;
    for(int i=0; i<group_by_refs_.size(); ++i) {
        contexts.push_back(std::make_shared<Context>());
    }

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this](Task* internal) {
            initialize_variables(internal);
        }),
        CreateLambdaTask([this, contexts](Task* internal) {
            // Fetch unique values for all Group By columns.
            for (int i=0; i<group_by_refs_.size(); i++) {
                // TODO(nicholas): No need for a TaskChain here
                internal->spawnTask(CreateTaskChain(
                    CreateLambdaTask([this, i](Task* internal) {
                        initialize_group_by_column(internal, i);
                    }),
                    CreateLambdaTask([this, i] {
                        all_unique_values_[i] = get_unique_values(group_by_refs_[i]).make_array();
                    }),
                    CreateLambdaTask([this, i, contexts](Task* internal) {

                        arrow::Datum out;
                        contexts[i]->match(internal, group_by_cols_[i], all_unique_values_[i], out);
                    }),
                    CreateLambdaTask([this, contexts, i] {
                        uniq_val_maps_[i] = contexts[i]->out_.chunked_array();
                    })
                ));
            }
        })
    ));
}

void Aggregate::compute_group_aggregate(
    Task* ctx,
    int agg_index,
    const std::vector<int>& group_id,
    const arrow::Datum agg_col) {


    if (num_aggs_ == 1) {
        auto aggregate = compute_aggregate(aggregate_refs_[0].kernel, agg_col.chunked_array());
        aggregates_vec_[agg_index] = std::static_pointer_cast<arrow::Int64Scalar>(aggregate.scalar())->value;
    }
    if (aggregates_vec_[agg_index] > 0) {
        arrow::Datum dummy;
        // Compute the aggregate over the filtered agg_col
        // Acquire builder_mutex_ so that groups are correctly associated with
        // their corresponding aggregates
        std::unique_lock<std::mutex> lock(builder_mutex_);
        insert_group_aggregate(dummy, agg_index);
        insert_group(group_id, agg_index);
    }
}

void Aggregate::compute_aggregates(Task *ctx) {

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this](Task* internal) {
            //TODO(nicholas): For now, we only perform one aggregate.
            auto table = aggregate_refs_[0].col_ref.table;
            auto col_name = aggregate_refs_[0].col_ref.col_name;
            agg_lazy_table_ = prev_result_->get_table(table);
            agg_lazy_table_.get_column_by_name(internal, col_name, agg_col_);
        }),
        CreateLambdaTask([this](Task* internal) {

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

            num_aggs_ = 1;

            // initialize group_id = {0, 0, ..., 0} and initialize maxes[i] to the number
            // of unique values in group by column i.
            for (int i = 0; i < n; i++) {
                group_id[i] = 0;
                maxes[i] = all_unique_values_[i]->length();
                unique_value_filters_[i].resize(maxes[i]);
                num_aggs_ *= maxes[i];
            }

            group_id_to_agg_index_map_.reserve(num_aggs_);
            group_filters_.resize(num_aggs_);
            filtered_agg_cols_.resize(num_aggs_);
            contexts_.resize(num_aggs_);
            unique_value_filter_contexts_.resize(num_aggs_);

            aggregates_vec_ = (std::atomic<int64_t>*) malloc(sizeof(std::atomic<int64_t>)*num_aggs_);
            for (int i=0; i<num_aggs_; ++i) {
                aggregates_vec_[i] = 0;
            }

            for (auto& vec : unique_value_filter_contexts_) {
                vec.resize(group_by_refs_.size());
            }

            int agg_index = 0;
            group_id_vec_.resize(num_aggs_);

            int index = n - 1; // loop index
            bool exit = false;
            while (!exit) {

                // LOOP BODY START
                group_id_vec_[agg_index] = group_id;
                auto key = 0;
                for (int k=0; k<group_id.size(); ++k) {
                    key += group_id[k]*pow(10, k);
                }
                group_id_to_agg_index_map_[key] = agg_index;
                ++agg_index;
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
                    ++group_id[index];
                }
                index = n - 1;
            }
            initialize_group_filters(internal);
        }),
        CreateLambdaTask([this](Task* internal) {
            int batch_size = num_aggs_ / std::thread::hardware_concurrency() /2;
            if (batch_size == 0) batch_size = num_aggs_;
            int num_batches = num_aggs_ / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
            if (num_batches == 0) num_batches = 1;

            for (int batch_i = 0; batch_i < num_batches; batch_i++) {
                // Each task gets the filter for one block and stores it in filter_vector
                internal->spawnLambdaTask([this, batch_i, batch_size](Task *internal) {
                    int base_i = batch_i * batch_size;
                    for (int i = base_i; i < base_i + batch_size && i < num_aggs_; i++) {
                        // i = agg_index
                        compute_group_aggregate(internal, i, group_id_vec_[i], agg_col_);
                    }
                });
            }
        })
    ));
}

void Aggregate::execute(Task *ctx) {

    ctx->spawnTask(CreateTaskChain(
        // Task 1 = Compute all aggregates
        CreateLambdaTask([this](Task *internal) {
            initialize(internal);
        }),
        CreateLambdaTask([this](Task *internal) {
            compute_aggregates(internal);
        }),
        CreateLambdaTask([this](Task *internal) {
//            initialize_group_filters(internal);
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

    // Assume that indices are correct and that boundschecking is unecessary.
    // CHANGE TO TRUE IF YOU ARE DEBUGGING
    arrow::compute::TakeOptions take_options(true);

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
        status = arrow::compute::Take(aggregates_, sorted_indices, take_options).Value(&aggregates_);
        evaluate_status(status, __FUNCTION__, __LINE__);


        for (auto &group: groups_) {
            status = arrow::compute::Take(group, sorted_indices, take_options).Value(&group);
            evaluate_status(status, __FUNCTION__, __LINE__);

        }
    }
}
} // namespace hustle