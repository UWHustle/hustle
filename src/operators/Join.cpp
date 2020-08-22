#include "Join.h"

#include <utility>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>
#include <arrow/scalar.h>
#include <utils/BloomFilter.h>
//#include <arrow/util/hashing.h>
//#include <arrow/compute/api_vector.h>

#define DEBUG 0
uint64_t PROBE_COUNT = 0;
uint64_t HIT_COUNT = 0;
uint64_t OPT_PROBE_COUNT = 0;

namespace hustle::operators {

Join::Join(
    const std::size_t query_id,
    std::vector<std::shared_ptr<OperatorResult>> prev_result,
    std::shared_ptr<OperatorResult> output_result,
    JoinGraph graph) : Operator(query_id) {

    prev_result_ = std::make_shared<OperatorResult>();
    prev_result_vec_ = std::move(prev_result);
    output_result_ = std::move(output_result);
    graph_ = std::move(graph);
    joined_indices_.resize(2);
    joined_index_chunks_.resize(2);

}

void Join::build_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &col, const std::shared_ptr<arrow::ChunkedArray> &filter, Task *ctx) {

    // NOTE: Do not forget to clear the hash table
    hash_table_.clear();

    // Precompute the row offsets of each chunk. A multithreaded build phase
    // requires that we know all offsets beforehand.
    std::vector<uint64_t> chunk_row_offsets(col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + col->chunk(i - 1)->length();
    }

    if (filter == nullptr) {
        hash_table_.reserve(col->length());

        for (int i = 0; i < col->num_chunks(); i++) {
            // Each task inserts one chunk into the hash table
            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

            for (int row = 0; row < chunk->length(); row++) {
                hash_table_[chunk->Value(row)] = {(uint32_t) chunk_row_offsets[i] + row, (uint16_t) i};
//                hash_table_[chunk->Value(row)] = chunk_row_offsets[i] + row;
            }
        }
    } else {

        uint32_t length_after_filtering = 0;

        for (int j=0; j<col->num_chunks(); ++j) {
            length_after_filtering += arrow::compute::internal::GetFilterOutputSize(*filter->chunk(j)->data(),arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
        }
        hash_table_.reserve(length_after_filtering);

        for (int i = 0; i < col->num_chunks(); i++) {
            // Each task inserts one chunk into the hash table
            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));
            auto chunkf = std::static_pointer_cast<arrow::BooleanArray>(filter->chunk(i));

            auto filter_data = filter->chunk(i)->data()->GetValues<uint8_t>(1, 0);

            for (int row = 0; row < chunk->length(); row++) {
                if (arrow::BitUtil::GetBit(filter_data, row)) {
//                    hash_table_[chunk->Value(row)] = chunk_row_offsets[i] + row;
                    hash_table_[chunk->Value(row)] = {(uint32_t) chunk_row_offsets[i] + row, (uint16_t) i};
                }
            }
        }
    }
}


void Join::probe_hash_table_block
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, const std::shared_ptr<arrow::ChunkedArray> &probe_filter, int batch_i, int batch_size, std::vector<uint64_t> chunk_row_offsets) {

    int base_i = batch_i * batch_size;
    auto hash_table_end = hash_table_.end();

    // TODO(nicholas): for now, we assume the join column is fixed width type, i.e. values are stored in the buffer at index 1.

    for (int i=base_i; i<base_i+batch_size && i<probe_col->num_chunks(); ++i) {

        arrow::Status status;

        int num_joined_indices = 0;
        auto offset = chunk_row_offsets[i];
        auto chunk = probe_col->chunk(i);
        auto chunk_length = chunk->length();
        auto left_join_chunk_data = chunk->data()->GetValues<uint64_t>(1, 0);
        auto filter_data = probe_filter->chunk(i)->data()->GetValues<uint8_t>(1, 0);

        // The indices of the rows joined in chunk i
        auto * joined_left_indices = (uint32_t*) malloc(sizeof(uint32_t)*chunk_length);
        auto * joined_right_indices = (uint32_t*) malloc(sizeof(uint32_t)*chunk_length);
        auto * joined_left_index_chunks = (uint16_t*) malloc(sizeof(uint16_t)*chunk_length);
        auto * joined_right_index_chunks = (uint16_t*) malloc(sizeof(uint16_t)*chunk_length);

        for (int row = 0; row < chunk_length; ++row) {
            if (arrow::BitUtil::GetBit(filter_data, row)) {
                auto key_value_pair = hash_table_.find(left_join_chunk_data[row]);
                if (key_value_pair != hash_table_end) {
                joined_left_indices[num_joined_indices] = row + offset;
                joined_left_index_chunks[num_joined_indices] = i;

                joined_right_indices[num_joined_indices] = key_value_pair->second.index;
                joined_right_index_chunks[num_joined_indices] = key_value_pair->second.chunk;
//                    joined_left_indices[num_joined_indices] = offset + row;  // insert left row index
//                    joined_right_indices[num_joined_indices] = key_value_pair->second; // insert right row index
                    ++num_joined_indices;
                }
            }
        }

        new_left_indices_vector[i] = std::vector<uint32_t>(joined_left_indices, joined_left_indices + num_joined_indices);
        new_right_indices_vector[i] = std::vector<uint32_t>(joined_right_indices, joined_right_indices + num_joined_indices);

        left_index_chunks_vector[i] = std::vector<uint16_t>(joined_left_index_chunks, joined_left_index_chunks + num_joined_indices);
        right_index_chunks_vector[i] = std::vector<uint16_t>(joined_right_index_chunks, joined_right_index_chunks + num_joined_indices);

        free(joined_left_indices);
        free(joined_right_indices);
        free(joined_left_index_chunks);
        free(joined_right_index_chunks);
    }
}

void Join::probe_hash_table_block
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, int batch_i, int batch_size, std::vector<uint64_t> chunk_row_offsets) {

    int base_i = batch_i * batch_size;
    auto hash_table_end = hash_table_.end();

    // TODO(nicholas): for now, we assume the join column is fixed width type, i.e. values are stored in the buffer at index 1.

    for (int i=base_i; i<base_i+batch_size && i<probe_col->num_chunks(); ++i) {

        arrow::Status status;

        int num_joined_indices = 0;
        auto offset = chunk_row_offsets[i];
        auto chunk = probe_col->chunk(i);
        auto chunk_length = chunk->length();
        auto left_join_chunk_data = chunk->data()->GetValues<uint64_t>(1, 0);


        // The indices of the rows joined in chunk i
        auto * joined_left_indices = (uint32_t*) malloc(sizeof(uint32_t)*chunk_length);
        auto * joined_right_indices = (uint32_t*) malloc(sizeof(uint32_t)*chunk_length);
        auto * joined_left_index_chunks = (uint16_t*) malloc(sizeof(uint16_t)*chunk_length);
        auto * joined_right_index_chunks = (uint16_t*) malloc(sizeof(uint16_t)*chunk_length);

        for (int row = 0; row < chunk_length; ++row) {
            auto key_value_pair = hash_table_.find(left_join_chunk_data[row]);
            if (key_value_pair != hash_table_end) {
                joined_left_indices[num_joined_indices] = row + offset;
                joined_left_index_chunks[num_joined_indices] = i;

                joined_right_indices[num_joined_indices] = key_value_pair->second.index;
                joined_right_index_chunks[num_joined_indices] = key_value_pair->second.chunk;
//                joined_left_indices[num_joined_indices] = offset + row;  // insert left row index
//                joined_right_indices[num_joined_indices] = key_value_pair->second; // insert right row index
                ++num_joined_indices;
            }
        }

        new_left_indices_vector[i] = std::vector<uint32_t>(joined_left_indices, joined_left_indices + num_joined_indices);
        new_right_indices_vector[i] = std::vector<uint32_t>(joined_right_indices, joined_right_indices + num_joined_indices);

        left_index_chunks_vector[i] = std::vector<uint16_t>(joined_left_index_chunks, joined_left_index_chunks + num_joined_indices);
        right_index_chunks_vector[i] = std::vector<uint16_t>(joined_right_index_chunks, joined_right_index_chunks + num_joined_indices);
//
        free(joined_left_indices);
        free(joined_right_indices);
        free(joined_left_index_chunks);
        free(joined_right_index_chunks);

    }
}

void Join::probe_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, const arrow::Datum &probe_filter, const arrow::Datum &probe_indices, Task *ctx) {

    new_left_indices_vector.resize(probe_col->num_chunks());
    new_right_indices_vector.resize(probe_col->num_chunks());
    right_index_chunks_vector.resize(probe_col->num_chunks());
    left_index_chunks_vector.resize(probe_col->num_chunks());

    // Precompute row offsets. A multithreaded probe phase requires that we know
    // all offsets beforehand.
    std::vector<uint64_t> chunk_row_offsets(probe_col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < probe_col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + probe_col->chunk(i - 1)->length();
    }

    int batch_size = probe_col->num_chunks() / std::thread::hardware_concurrency() / 2;
    if (batch_size == 0) batch_size = probe_col->num_chunks();
    int num_batches = probe_col->num_chunks() / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
    if (num_batches == 0) num_batches = 1;

    // Probe phase
    for (int batch_i=0; batch_i<num_batches; batch_i++) {

        // Each task probes one chunk
        ctx->spawnLambdaTask([this, batch_i, batch_size, probe_col, probe_filter, probe_indices, chunk_row_offsets] {
            switch(probe_filter.kind()) {
                case arrow::Datum::CHUNKED_ARRAY: {
                    probe_hash_table_block(probe_col, probe_filter.chunked_array(), batch_i, batch_size,
                                           chunk_row_offsets);
                    break;
                }
                default: {
                    probe_hash_table_block(probe_col, batch_i, batch_size, chunk_row_offsets);
                }
            }
        });
    }
}

void Join::finish_probe(Task* ctx) {

    int num_indices = 0;
    for (auto& vec : new_left_indices_vector) {
        num_indices += vec.size();
    }

//    std::cout << num_indices << std::endl;
    ctx->spawnLambdaTask([this, num_indices] {
        arrow::Status status;


//        arrow::ArrayVector vec;
//        arrow::ArrayVector index_chunks_vec;
//        vec.resize(new_left_indices_vector.size());
//        index_chunks_vec.resize(new_left_indices_vector.size());
//
//        arrow::UInt16Builder b16;
        arrow::UInt32Builder new_left_indices_builder;
        std::shared_ptr<arrow::UInt32Array> new_left_indices;

        status = new_left_indices_builder.Reserve(num_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // TODO(nicholas): Use UnsafeAppend or index access
        // Append all of the indices to an ArrayBuilder.
        for (int i = 0; i < new_left_indices_vector.size(); i++) {
            status = new_left_indices_builder.AppendValues(new_left_indices_vector[i]);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//            status = new_left_indices_builder.Finish(&vec[i]);
//            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//
//            status = b16.AppendValues(left_index_chunks_vector[i]);
//            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//            status = b16.Finish(&index_chunks_vec[i]);
//            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }

//        joined_indices_[0] = std::make_shared<arrow::ChunkedArray>(vec);
//        joined_index_chunks_[0] = std::make_shared<arrow::ChunkedArray>(index_chunks_vec);

        status = new_left_indices_builder.Finish(&new_left_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

//        std::make_shared<arrow::ChunkedArray>(vec);
        joined_indices_[0] = (arrow::Datum(new_left_indices));
    });
    ctx->spawnLambdaTask([this, num_indices] {
        arrow::Status status;

        arrow::UInt32Builder new_right_indices_builder;
        std::shared_ptr<arrow::UInt32Array> new_right_indices;

        status = new_right_indices_builder.Reserve(num_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // TODO(nicholas): Use UnsafeAppend or index access
        // Append all of the indices to an ArrayBuilder.
        for (int i = 0; i < new_right_indices_vector.size(); i++) {
            status = new_right_indices_builder.AppendValues(
                new_right_indices_vector[i]);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }

        status = new_right_indices_builder.Finish(&new_right_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        joined_indices_[1] = (arrow::Datum(new_right_indices));
    });
}

std::shared_ptr<OperatorResult>
Join::back_propogate_result(LazyTable& left, LazyTable right,
                            const std::vector<arrow::Datum>& joined_indices) {

    arrow::Status status;
    arrow::Datum new_indices;
    arrow::Datum new_index_chunks;

    std::vector<LazyTable> output_lazy_tables;

    // The indices of the indices that were joined
    auto left_indices_of_indices = joined_indices_[0];
    auto right_indices_of_indices = joined_indices_[1];

    auto left_index_chunks = joined_index_chunks_[0];
    auto right_index_chunks = joined_index_chunks_[1];

    // Assume that indices are correct and that boundschecking is unecessary.
    // CHANGE TO TRUE IF YOU ARE DEBUGGING
    arrow::compute::TakeOptions take_options(true);

    // Update the indices of the left LazyTable. If there was no previous
    // join on the left table, then left_indices_of_indices directly
    // corresponds to indices in the left table, and we do not need to
    // call Take.

    if (left.indices.kind() != arrow::Datum::NONE) {
        status = arrow::compute::Take(left.indices, left_indices_of_indices, take_options).Value(&new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    } else {
        new_indices = left_indices_of_indices;
    }
    if (left.index_chunks.kind() != arrow::Datum::NONE) {
        status = arrow::compute::Take(left.index_chunks, left_indices_of_indices, take_options).Value(&new_index_chunks);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    } else {
        new_index_chunks = left_index_chunks;
    }
    output_lazy_tables.emplace_back(left.table, left.filter, new_indices, new_index_chunks);


    // Update the indices of the right LazyTable. If there was no previous
    // join on the right table, then right_indices_of_indices directly
    // corresponds to indices in the right table, and we do not need to
    // call Take.
    if (right.indices.kind() != arrow::Datum::NONE) {
        status = arrow::compute::Take(right.indices, right_indices_of_indices, take_options).Value(&new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    } else {
        new_indices = right_indices_of_indices;
    }
//    if (right.index_chunks.kind() != arrow::Datum::NONE) {
//        status = arrow::compute::Take(right.index_chunks, right_indices_of_indices, take_options).Value(&new_index_chunks);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//    } else {
//        new_index_chunks = right_index_chunks;
//    }
    new_index_chunks = arrow::Datum();
    output_lazy_tables.emplace_back(right.table, right.filter, new_indices, new_index_chunks);

    // Propogate the join to the other tables in the previous OperatorResult.
    // This elimates tuples from other tables in the previous result that were
    // eliminated in the most recent join.
    for (auto &lazy_table : prev_result_->lazy_tables_) {
        if (lazy_table.table != left.table &&
            lazy_table.table != right.table) {
            if (finished_[lazy_table.table]) {

                status = arrow::compute::Take(lazy_table.indices, left_indices_of_indices, take_options).Value(&new_indices);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                output_lazy_tables.emplace_back(
                    lazy_table.table, lazy_table.filter, new_indices, arrow::Datum());
            } else {
//            std::cout << lazy_table.indices.length() << std::endl;

            output_lazy_tables.emplace_back(
                    lazy_table.table, lazy_table.filter, lazy_table.indices, lazy_table.index_chunks);
            }
        }
    }

    return std::make_shared<OperatorResult>(output_lazy_tables);
}


void Join::hash_join(int i, Task *ctx) {

    // Join lefts[i] with rights[i].
    // Why pass in an index i instead of the actual left and right tables?
    // If we pass the tables to the lambda expression by value, then updates
    // made to the index arrays will not be seen by downstream joins. If we
    // pass the tables by reference, then we get a nullptr exception, since
    // the left and right tables would go out of scope before the Task can be
    // executed. To get around this issue, we store the left and right tables
    // in vectors and access them by index. Because the vectors are class
    // variables (and because we pass `this` by reference), updates to the index
    // arrays will be seen by downstream joins, and we don't have to worry about
    // anything going out of scope.
    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this, i](Task *internal) {
            left_ = prev_result_->get_table(lefts[i].table);
            right_ = prev_result_->get_table(rights[i].table);
            left_.get_column_by_name(internal, left_col_names[i], left_join_col_);
            right_.get_column_by_name(internal, right_col_names[i], right_join_col_);
        }),
        CreateLambdaTask([this, i](Task *internal) {
            // Build phase
            switch(right_.filter.kind()) {
                case arrow::Datum::CHUNKED_ARRAY: {
                    build_hash_table(right_join_col_.chunked_array(), right_.filter.chunked_array(), internal);
                    break;
                }
                default: {
                    build_hash_table(right_join_col_.chunked_array(), nullptr, internal);
                }
            }
        }),
        CreateLambdaTask([this, i](Task *internal) {
            // Probe phase
            probe_hash_table(left_join_col_.chunked_array(), left_.filter, left_.indices, internal);
            
        }),
        CreateLambdaTask([this, i](Task *internal) {
            finish_probe(internal);
            finished_[lefts[i].table] = true;
            finished_[rights[i].table] = true;
        }),
        CreateLambdaTask([this, i](Task *internal) {
            auto left = prev_result_->get_table(lefts[i].table);
            auto right = prev_result_->get_table(rights[i].table);
            // Update indices of other LazyTables in the previous OperatorResult
            prev_result_ = back_propogate_result(left, right, joined_indices_);
        })
    ));
}

void Join::execute(Task *ctx) {

    for (auto &result : prev_result_vec_) {
        prev_result_->append(result);
    }
    // To handle a variable number of joins, we must store the tasks beforehand. The variadic CreateTaskChain
    // cannot help us here!

    // TODO(nicholas): For now, we assume joins have simple predicates
    //   without connective operators.
    auto predicates = graph_.get_predicates(0);

    // Loop over the join predicates and store the left/right LazyTables and the
    // left/right join column names
    for (auto &jpred : predicates) {

        auto left_ref = jpred.left_col_ref_;
        auto right_ref = jpred.right_col_ref_;

        left_col_names.push_back(left_ref.col_name);
        right_col_names.push_back(right_ref.col_name);

        fact_fk_col_names_.push_back(left_ref.col_name);
        dim_pk_col_names_.push_back(right_ref.col_name);
        dim_pk_cols_[right_ref.col_name] = arrow::Datum();

        // The previous result prev may contain many LazyTables. Find the
        // LazyTables that we want to join.
        for (int i = 0; i < prev_result_->lazy_tables_.size(); i++) {
            auto lazy_table = prev_result_->get_table(i);
            finished_[lazy_table.table] = false;

            if (left_ref.table == lazy_table.table) {
                lefts.push_back(lazy_table);
                fact_table_ = lazy_table; // left table is always the same
                if (lazy_table.indices.kind() != arrow::Datum::NONE) fact_indices_ = lazy_table.indices.array()->GetValues<uint32_t>(1, 0);
            } else if (right_ref.table == lazy_table.table) {
                rights.push_back(lazy_table);
                dim_tables_.push_back(lazy_table);
            }
        }
    }

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this](Task *internal) {
                initialize(internal);
                build_filters(internal);
            }),
        CreateLambdaTask([this](Task *internal) {
                // Grab any fact table column so we can pre-compute chunk row offsets.
                for (auto& name: fact_fk_col_names_) {
                    fact_fk_cols2_[name] =fact_fk_cols_[name].chunked_array();
                }
                auto fact_col = fact_fk_cols_[fact_fk_col_names_[0]].chunked_array();
                lip_indices_.resize(fact_col->num_chunks());

                chunk_row_offsets_.resize(fact_col->num_chunks());
                chunk_row_offsets_[0] = 0;
                for (int i = 1; i < fact_col->num_chunks(); i++) {
                    chunk_row_offsets_[i] =
                        chunk_row_offsets_[i - 1] + fact_col->chunk(i - 1)->length();
                }
                probe_filters(internal);
            }),
        CreateLambdaTask([this]() {
                finish_lip();
                prev_result_ = output_result_lip_;
                if (DEBUG) {
                    std::cout << "LIP DEBUG:" << std::endl;
                    std::cout << "\t hit count:\t\t\t\t\t\t" <<  HIT_COUNT << std::endl;
                    std::cout << "\t probe count:\t\t\t\t\t" <<  PROBE_COUNT << std::endl;
                    std::cout << "\t hit count / probe count:\t\t" << ((double) HIT_COUNT)/PROBE_COUNT << std::endl;
                    std::cout << "\t opt hit count:\t\t\t\t\t" <<  OPT_PROBE_COUNT << std::endl;
                    std::cout << "\t probe count / opt probe count:\t" << ((double) PROBE_COUNT)/OPT_PROBE_COUNT << std::endl;
                }
            }),
        CreateLambdaTask([this](Task* internal) {
            std::vector<Task *> tasks;

            for (int i = 0; i < lefts.size(); i++) {
                tasks.push_back(CreateLambdaTask(
                    [this, i](Task *internal) {
                        hash_join(i, internal);
                    }));
            }

            tasks.push_back(
                CreateLambdaTask([this]() {
                    finish();
                }));

            // The (j+1)st task in tasks is not executed until the jth task finishes.
            internal->spawnTask(CreateTaskChain(tasks));

            })
    ));

    // Each task is one join

}

void Join::finish() {
    // Must append to output_result_ first
    output_result_->append(prev_result_);
//    prev_result_->append(prev_result_);

}


void Join::build_filters(Task* ctx) {

    dim_filters_.resize(dim_tables_.size());

    for (int i=0; i<dim_tables_.size(); i++) {
        auto dim_join_col_name = dim_pk_col_names_[i];
        // Task = build the Bloom filter for one dimension table.
        ctx->spawnTask(CreateTaskChain(
            CreateLambdaTask([this, dim_join_col_name, i](Task* internal) {
                dim_pk_cols_[dim_join_col_name] = dim_tables_[i].table->get_column_by_name(dim_join_col_name);
            }),
            CreateLambdaTask([this, dim_join_col_name, i] {
                auto pk_col = dim_pk_cols_[dim_join_col_name].chunked_array();
                auto filter_d = dim_tables_[i].filter;
                std::shared_ptr<BloomFilter> bloom_filter;

                // TODO(nicholas): consider indices as well. We don't have to worry about this for SSB, though.
                auto indices_d = dim_tables_[i].indices;

                if (filter_d.kind() == arrow::Datum::NONE) {
                    bloom_filter = std::make_shared<BloomFilter>(pk_col->length());
                    for (int j=0; j<pk_col->num_chunks(); ++j) {
                        // TODO(nicholas): For now, we assume the column is of INT64 type.
                        auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
                        for (int k = 0; k < chunk->length(); ++k) {
                            bloom_filter->insert(chunk->Value(k));
                        }
                    }
                } else {
                    auto filter = filter_d.chunked_array();
                    uint32_t length_after_filtering = 0;

                    for (int j=0; j<pk_col->num_chunks(); ++j) {
                        length_after_filtering += arrow::compute::internal::GetFilterOutputSize(*filter->chunk(j)->data(),arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
                    }
                    bloom_filter = std::make_shared<BloomFilter>(length_after_filtering);

                    for (int j=0; j<pk_col->num_chunks(); ++j) {
                        // TODO(nicholas): For now, we assume the column is of INT64 type.
                        auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));
                        auto chunkf = std::static_pointer_cast<arrow::BooleanArray>(filter->chunk(j));

                        for (int k=0; k<chunk->length(); ++k) {
                            if (chunkf->Value(k)) {
                                bloom_filter->insert(chunk->Value(k));
                            }
                        }
                    }
                }

                bloom_filter->set_memory(1);
                bloom_filter->set_fact_fk_name(fact_fk_col_names_[i]);
                dim_filters_[i] = (bloom_filter);
            })
        ));
    }
}

void Join::probe_filters(int chunk_start, int chunk_end, int filter_j, Task* ctx) {

    // indices[i] stores the indices of fact table rows that passed the
    // ith filter.

    for (auto chunk_i=chunk_start; chunk_i<=chunk_end; ++chunk_i) {

        ctx->spawnLambdaTask([this, chunk_i, filter_j] {

            auto bloom_filter = dim_filters_[filter_j];
            auto fact_fk_col = fact_fk_cols2_[bloom_filter->get_fact_fk_name()];

            uint32_t *indices = nullptr;
            int32_t indices_length = -1;
            uint32_t offset = chunk_row_offsets_[chunk_i];
            uint32_t temp;

            // TODO(nicholas): For now, we assume the column is of INT64 type
            auto chunk = fact_fk_col->chunk(chunk_i); // @bug: fact_fk_col is nullptr
            auto chunk_data = chunk->data()->GetValues<int64_t>(1, 0);
            auto chunk_length = chunk->length();

            // For the first filter, we must probe all rows of the block.
            if (filter_j == 0) {
                // Reserve space for the first index vector
                int k = 0;
                indices = (uint32_t *) malloc(sizeof(uint32_t) * chunk_length);

                for (int row = 0; row < chunk_length; ++row) {

                    if (bloom_filter->probe(chunk_data[row])) {
                        indices[k++] = row + offset;
                    }
                }
                indices_length = k - 1;
                lip_indices_[chunk_i] = std::vector<uint32_t>(indices, indices + indices_length + 1);

                bloom_filter->probe_count_ += chunk_length;
                bloom_filter->hit_count_ += indices_length+1;
                if (DEBUG) {
                    PROBE_COUNT += chunk_length;
                    HIT_COUNT += indices_length+1;
                }

                free(indices);
            }

                // For the remaining filters, we only need to probe rows that passed
                // the previous filters.
            else {
                indices = lip_indices_[chunk_i].data();
                indices_length = lip_indices_[chunk_i].size()-1;
                bloom_filter->probe_count_ += indices_length+1;
                if (DEBUG) PROBE_COUNT += indices_length+1;

                int k = 0;
                while (k <= indices_length) {
                    if (bloom_filter->probe(chunk_data[indices[k] - offset])) {
                        ++k;
                    } else {
                        temp = indices[k];
                        indices[k] = indices[indices_length];
                        indices[indices_length--] = temp;
                    }
                }
                lip_indices_[chunk_i].resize(indices_length+1);
                bloom_filter->hit_count_ += indices_length+1;
                if (DEBUG) HIT_COUNT += indices_length+1;
            }
        });
    }
}

void Join::probe_filters(Task *ctx) {

    int num_chunks = fact_fk_cols_[fact_fk_col_names_[0]].chunked_array()->num_chunks();
    batch_size_ = std::thread::hardware_concurrency();

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

            std::vector<Task*> tasks;
            for (int filter_j = 0; filter_j < dim_tables_.size(); ++filter_j) {

                auto probe_task_one_filter = CreateLambdaTask([this, base_i, filter_j, num_chunks](Task* internal) {
                    if (base_i + batch_size_ - 1 < num_chunks) {
                        probe_filters(base_i, base_i + batch_size_ - 1, filter_j, internal);
                    }
                    else {
                        probe_filters(base_i, num_chunks-1, filter_j, internal);
                    }
                });

                tasks.push_back(probe_task_one_filter);
            }
            internal->spawnTask(CreateTaskChain(tasks));
        });

        // Task 2 = update Bloom filter statistics and sort filters accordingly
        auto update_and_sort_task = CreateLambdaTask([this] {
            for (int i=0; i<dim_filters_.size(); ++i) {
                dim_filters_[i]->update();
//                std::cout << dim_filters_[i]->get_fact_fk_name() << " " << dim_filters_[i]->get_hit_rate() << " " << dim_filters_[i]->get_hit_rate_q(75) << " " << dim_filters_[i]->get_hit_rate_estimate(75) << " ";
            }
//            std::cout << std::endl;
            std::sort(dim_filters_.begin(), dim_filters_.end(), BloomFilter::compare);
        });

        // Require that Task 2 start only after Task 1 is finished. Each task
        // in tasks probes one batch and then sorts the filters.
        tasks.push_back(CreateTaskChain(probe_task, update_and_sort_task));
    }

    ctx->spawnTask(CreateTaskChain(tasks));
}

void Join::finish_lip() {
    arrow::Status status;
    arrow::UInt32Builder new_indices_builder;
    std::shared_ptr<arrow::UInt32Array> new_indices;

    // Append all of the LIP indices to an ArrayBuilder.
    for (int i = 0; i < lip_indices_.size(); i++) {
        status = new_indices_builder.AppendValues(lip_indices_[i]);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        if (DEBUG) OPT_PROBE_COUNT += lip_indices_[i].size();
    }

    if (DEBUG) {
        int num_lip_indices = 0;
        for (int i = 0; i < lip_indices_.size(); i++) {
            num_lip_indices += lip_indices_[i].size();
        }

        OPT_PROBE_COUNT = num_lip_indices*dim_filters_.size(); // A hit will pass all filters
        OPT_PROBE_COUNT += fact_table_.table->get_num_rows() - num_lip_indices; // opt scenario: we eliminate bad tuples with one probe.
    }


    // Construct new fact table index array
    status = new_indices_builder.Finish(&new_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    // Create a new lazy fact table with the new index array
    LazyTable result_unit(fact_table_.table, fact_table_.filter, new_indices);
    OperatorResult result({result_unit});
    output_result_lip_ = std::make_shared<OperatorResult>();
    output_result_lip_->append(std::make_shared<OperatorResult>(result));

    // Add all dimension tables to the output without changing them.
    for (int i=0; i<dim_tables_.size(); i++) {
        output_result_lip_->append(dim_tables_[i]);
    }
}

void Join::initialize(Task* ctx) {
    for (int i=0; i<dim_tables_.size(); i++) {
        auto fact_join_col_name = fact_fk_col_names_[i];
        fact_fk_cols_.emplace(fact_join_col_name, arrow::Datum());
    }

    // Pre-materialized and save fact table fk columns.
    for (int i=0; i<dim_tables_.size(); i++) {
        ctx->spawnLambdaTask([this, i](Task* internal) {
            auto fact_join_col_name = fact_fk_col_names_[i];
            fact_table_.get_column_by_name(internal, fact_join_col_name, fact_fk_cols_[fact_join_col_name]);
        });
    }
}





} // namespace hustle