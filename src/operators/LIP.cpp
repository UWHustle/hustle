//
// Created by Nicholas Corrado on 5/21/20.
//

#include "LIP.h"
#include "../table/util.h"

namespace hustle::operators {

LIP::LIP(const std::size_t query_id,
         std::vector<std::shared_ptr<OperatorResult>> prev_result_vec,
         std::shared_ptr<OperatorResult> output_result,
         hustle::operators::JoinGraph graph) : Operator(query_id) {

    prev_result_ = std::make_shared<OperatorResult>();
    prev_result_vec_ = prev_result_vec;
    output_result_ = std::move(output_result);
    graph_ = std::move(graph);
}

void LIP::build_filters(Task* ctx) {

    dim_filters_.resize(dim_tables_.size());

    for (int i=0; i<dim_tables_.size(); i++) {
        auto dim_join_col_name = dim_pk_col_names_[i];
        // Task = build the Bloom filter for one dimension table.
        ctx->spawnTask(CreateTaskChain(
            CreateLambdaTask([this, dim_join_col_name, i](Task* internal) {
//                dim_tables_[i].get_column_by_name(internal, dim_join_col_name, dim_pk_cols_[dim_join_col_name]);
                dim_pk_cols_[dim_join_col_name] = dim_tables_[i].table->get_column_by_name(dim_join_col_name);
            }),
            CreateLambdaTask([this, dim_join_col_name, i] {
                auto pk_col = dim_pk_cols_[dim_join_col_name].chunked_array();
                auto bloom_filter = std::make_shared<BloomFilter>(pk_col->length());
                auto filter_d = dim_tables_[i].filter;
                // TODO(nicholas): consider indices as well. We don't have to worry about this for SSB, though.
                auto indices_d = dim_tables_[i].indices;

                if (filter_d.kind() == arrow::Datum::NONE) {
                    for (int j=0; j<pk_col->num_chunks(); j++) {
                        // TODO(nicholas): For now, we assume the column is of INT64 type.
                        auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
                        for (int k = 0; k < chunk->length(); k++) {
                            auto val = chunk->Value(k);
                            bloom_filter->insert(val);
                        }
                    }
                } else {
                    auto filter = filter_d.chunked_array();
                    for (int j=0; j<pk_col->num_chunks(); j++) {
                        // TODO(nicholas): For now, we assume the column is of INT64 type.
                        auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
                        auto chunkf = std::static_pointer_cast<arrow::BooleanArray>(filter->chunk(j));

                        for (int k=0; k<chunk->length(); k++) {
                            if (chunkf->Value(k)) {
                                auto val = chunk->Value(k);
                                bloom_filter->insert(val);
                            }
                        }
                    }
                }


                bloom_filter->set_memory(10000);
                bloom_filter->set_fact_fk_name(fact_fk_col_names_[i]);
                dim_filters_[i] = (bloom_filter);
            })
        ));
    }
}

void LIP::probe_filters2(int chunk_i) {

    // indices[i] stores the indices of fact table rows that passed the
    // ith filter.
    uint64_t* indices = nullptr;
    int64_t indices_length = -1;
    uint64_t offset = chunk_row_offsets_[chunk_i];
    uint64_t temp;
    for (int filter_j = 0; filter_j < dim_tables_.size(); ++filter_j) {

        auto bloom_filter = dim_filters_[filter_j];

        auto fact_fk_col = fact_fk_cols2_[bloom_filter->get_fact_fk_name()];
        // TODO(nicholas): For now, we assume the column is of INT64 type
        auto chunk = fact_fk_col->chunk(chunk_i);
        auto chunk_data = chunk->data()->GetValues<int64_t>(1, 0);
        auto chunk_length = chunk->length();
        auto max_row = chunk_length + offset;

        // For the first filter, we must probe all rows of the block.
        if (filter_j == 0) {
            // Reserve space for the first index vector
            int k=0;
            indices = (uint64_t*) malloc(sizeof(uint64_t)*chunk_length);

            for (int row = offset; row < max_row; ++row) {

                if (bloom_filter->probe(chunk_data[row])) {
                    indices[k] = fact_indices_[row];
                    ++k;
                }
            }
            indices_length = k-1;
        }

        // For the remaining filters, we only need to probe rows that passed
        // the previous filters.
        else {
            int k=0;
            while (k<=indices_length) {
                if (bloom_filter->probe(chunk_data[indices[k] - offset])) {
                    ++k;
                }
                else {
                    temp = indices[k];
                    indices[k] = indices[indices_length];
                    indices[indices_length] = temp;
                    --indices_length;
                }
            }
        }
    }

    lip_indices_[chunk_i] = std::vector<uint64_t>(indices, indices + indices_length+1 );
}

void LIP::probe_filters(int chunk_i) {

    // indices[i] stores the indices of fact table rows that passed the
    // ith filter.
    uint64_t* indices = nullptr;
    int64_t indices_length = -1;
    uint64_t offset = chunk_row_offsets_[chunk_i];
    uint64_t temp;
    for (int filter_j = 0; filter_j < dim_tables_.size(); ++filter_j) {

        auto bloom_filter = dim_filters_[filter_j];

        auto fact_fk_col = fact_fk_cols2_[bloom_filter->get_fact_fk_name()];
        // TODO(nicholas): For now, we assume the column is of INT64 type
        auto chunk = fact_fk_col->chunk(chunk_i);
        auto chunk_data = chunk->data()->GetValues<int64_t>(1, 0);
        auto chunk_length = chunk->length();

        // For the first filter, we must probe all rows of the block.
        if (filter_j == 0) {
            // Reserve space for the first index vector
            int k=0;
//            uint64_t temp_indices[sizeof(uint64_t)*chunk_length];
//            indices = temp_indices;
            indices = (uint64_t*) malloc(sizeof(uint64_t)*chunk_length);

            for (int row = 0; row < chunk_length; ++row) {

                if (bloom_filter->probe(chunk_data[row])) {
                    indices[k++] = row + offset;
                }
            }
            indices_length = k-1;
        }

        // For the remaining filters, we only need to probe rows that passed
        // the previous filters.
        else {
            int k=0;
            while (k<=indices_length) {
                if (bloom_filter->probe(chunk_data[indices[k] - offset])) {
                    ++k;
                }
                else {
                    temp = indices[k];
                    indices[k] = indices[indices_length];
                    indices[indices_length--] = temp;
                }
            }
        }
    }

    lip_indices_[chunk_i] = std::vector<uint64_t>(indices, indices + indices_length+1);
}

void LIP::probe_filters(Task *ctx) {

    int num_chunks = fact_fk_cols_[fact_fk_col_names_[0]].chunked_array()->num_chunks();
    batch_size_ = 60;

    batch_size_ = num_chunks / 8;
    if (batch_size_ == 0) batch_size_ = num_chunks;
    int num_batches = num_chunks / batch_size_ + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
    if (num_batches == 0) num_batches = 1;

    std::vector<Task *> tasks;
    tasks.reserve(num_batches);

    for (int batch_i=0; batch_i<num_batches; batch_i++) {

        // Task 1 = probe all filters with one batch of blocks, where each batch
        // contains batch_size_ blocks.
        auto probe_task = CreateLambdaTask([this, batch_i, num_chunks](Task *internal) {
            int base_i = batch_i * batch_size_;
            for (int i=base_i; i<base_i + batch_size_ && i<num_chunks; i++) {
                internal->spawnLambdaTask([this, i] {
                    if (fact_indices_ == nullptr) {
                        probe_filters(i);
                    }else {
                        probe_filters2(i);
                    }
                });
           }
        });

        // Task 2 = update Bloom filter statistics and sort filters accordingly
        auto update_and_sort_task = CreateLambdaTask([this] {
            for (auto &bloom_filter: dim_filters_) bloom_filter->update();
            std::sort(dim_filters_.begin(), dim_filters_.end(), BloomFilter::compare);
        });

        // Require that Task 2 start only after Task 1 is finished. Each task
        // in tasks probes one batch and then sorts the filters.
        tasks.push_back(CreateTaskChain(probe_task, update_and_sort_task));
    }

    ctx->spawnTask(CreateTaskChain(tasks));
}

void LIP::finish() {
    arrow::Status status;
    arrow::UInt64Builder new_indices_builder;
    std::shared_ptr<arrow::UInt64Array> new_indices;

    // Append all of the LIP indices to an ArrayBuilder.
    for (int i = 0; i < lip_indices_.size(); i++) {
        status = new_indices_builder.AppendValues(lip_indices_[i]);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }

    // Construct new fact table index array
    status = new_indices_builder.Finish(&new_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    // Create a new lazy fact table with the new index array
    LazyTable result_unit(fact_table_.table, fact_table_.filter, new_indices);
    OperatorResult result({result_unit});
    output_result_->append(std::make_shared<OperatorResult>(result));

    // Add all dimension tables to the output without changing them.
    for (int i=0; i<dim_tables_.size(); i++) {
        output_result_->append(dim_tables_[i]);
    }
}

void LIP::initialize(Task* ctx) {
    for (int i=0; i<dim_tables_.size(); i++) {
        auto fact_join_col_name = fact_fk_col_names_[i];
        fact_fk_cols_[fact_join_col_name] = arrow::Datum();
    }

    // Pre-materialized and save fact table fk columns.
    for (int i=0; i<dim_tables_.size(); i++) {
        ctx->spawnLambdaTask([this, i](Task* internal) {
            auto fact_join_col_name = fact_fk_col_names_[i];
            fact_table_.get_column_by_name(internal, fact_join_col_name, fact_fk_cols_[fact_join_col_name]);
        });
    }
}

void LIP::execute(Task *ctx) {


    for (auto &result : prev_result_vec_) {
        prev_result_->append(result);
    }
    auto predicates = graph_.get_predicates(0);

    // Loop over the join predicates and store the left/right LazyTables and the
    // left/right join column names
    for (auto &jpred : predicates) {

        auto left_ref = jpred.left_col_ref_;
        auto right_ref = jpred.right_col_ref_;

        fact_fk_col_names_.push_back(left_ref.col_name);
        dim_pk_col_names_.push_back(right_ref.col_name);
        dim_pk_cols_[right_ref.col_name] = arrow::Datum();

        // The previous result prev may contain many LazyTables. Find the
        // LazyTables that we want to join.
        for (int i = 0; i < prev_result_->lazy_tables_.size(); i++) {
            auto lazy_table = prev_result_->get_table(i);

            if (left_ref.table == lazy_table.table) {
                fact_table_ = lazy_table; // left table is always the same
                if (lazy_table.indices.kind() != arrow::Datum::NONE) fact_indices_ = lazy_table.indices.array()->GetValues<uint64_t>(1, 0);
                else fact_indices_ = nullptr;
            } else if (right_ref.table == lazy_table.table) {
                dim_tables_.push_back(lazy_table);
            }
        }
    }

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask(
        [this](Task *internal) {
            initialize(internal);
            build_filters(internal);
        }),
        CreateLambdaTask(
        [this](Task *internal) {
            // Grab any fact table column so we can pre-compute chunk row offsets.
            for (auto& name: fact_fk_col_names_) {
                fact_fk_cols2_[name] =fact_fk_cols_[name].chunked_array();
            }
            auto fact_col = fact_fk_cols_[fact_fk_col_names_[0]].chunked_array();
            lip_indices_.resize(fact_col->num_chunks());

//            auto f = fact_table_.table->get_column(0);
            chunk_row_offsets_.resize(fact_col->num_chunks());
            chunk_row_offsets_[0] = 0;
            for (int i = 1; i < fact_col->num_chunks(); i++) {
                chunk_row_offsets_[i] =
                    chunk_row_offsets_[i - 1] + fact_col->chunk(i - 1)->length();
            }
            probe_filters(internal);
        }),
        CreateLambdaTask(
            [this]() {
                finish();
            })
    ));

    // TODO(nicholas): for now, we assume that there is no need to backpropogate
    //  the LIP result. This would be an issue only if we run LIP on a left deep
    //  subplan.

}


}