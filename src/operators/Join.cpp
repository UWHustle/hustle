#include "Join.h"

#include <utility>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>
#include <arrow/scalar.h>
//#include <arrow/util/hashing.h>
//#include <arrow/compute/api_vector.h>

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

}

void Join::build_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &col, Task *ctx) {

    // NOTE: Do not forget to clear the hash table
    hash_table_.clear();
    hash_table_.reserve(col->length());

    // Precompute the row offsets of each chunk. A multithreaded build phase
    // requires that we know all offsets beforehand.
    std::vector<uint64_t> chunk_row_offsets(col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + col->chunk(i - 1)->length();
    }

//    arrow::internal::HashTable<int64_t> h(arrow::default_memory_pool(), right_join_col_.length());


    for (int i = 0; i < col->num_chunks(); i++) {
        // Each task inserts one chunk into the hash table
            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                col->chunk(i));

            for (int row = 0; row < chunk->length(); row++) {
                hash_table_[chunk->Value(row)] = chunk_row_offsets[i] + row;
            }
    }
}

void Join::probe_hash_table_block
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, int batch_i, int batch_size, std::vector<uint64_t> chunk_row_offsets) {

    int base_i = batch_i * batch_size;
    auto hash_table_end = hash_table_.end();

    for (int i=base_i; i<base_i+batch_size && i<probe_col->num_chunks(); i++) {

        arrow::Status status;

        int num_joined_indices = 0;
        auto chunk = probe_col->chunk(i);
        auto chunk_length = chunk->length();

        // The indices of the rows joined in chunk i
        int64_t joined_left_indices[chunk_length];
        int64_t joined_right_indices[chunk_length];

        arrow::Datum match_result;
        arrow::Datum not_null;
        arrow::Datum left_joined;
        arrow::Datum right_joined;
        arrow::Datum offset;

        // NOTE: This only makes sense if right_join_col_ does not contain duplicate values.
//        status = arrow::compute::Match(right_join_col_, chunk).Value(&match_result);
        status = arrow::compute::Match(chunk, right_join_col_).Value(&match_result);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // Result of match is INT32
        auto match_result_array = std::static_pointer_cast<arrow::Int32Array>(match_result.make_array());
        auto match_result_length = match_result_array->length();
        for (int k=0; k<match_result_length; k++) {
            if (!match_result_array->IsNull(k)){
                joined_left_indices[num_joined_indices] = k + chunk_row_offsets[i];
                joined_right_indices[num_joined_indices] = (uint64_t) match_result_array->Value(k);
                ++num_joined_indices;
            }
        }




//        status = arrow::compute::IsNull(match_result).Value(&not_null);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//        status = arrow::compute::Invert(not_null).Value(&not_null);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//
//        status = arrow::compute::Filter(match_result, not_null).Value(&right_joined);
////        std::cout << right_joined.make_array()->ToString() << std::endl;
//
//        status = arrow::compute::internal::GetTakeIndices(*not_null.array(), arrow::compute::FilterOptions::EMIT_NULL).Value(&left_joined);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

//        // TODO(nicholas): for now, we assume the join column is fixed width type, i.e. values are stored in the buffer at index 1.
//        auto left_join_chunk_data = chunk->data()->GetValues<uint64_t>(1, 0);
//
//        for (int row = 0; row < chunk_length; row++) {
//
//            auto key_value_pair = hash_table_.find(left_join_chunk_data[row]);
//            if ( key_value_pair != hash_table_end) {
//                joined_left_indices[num_joined_indices] = chunk_row_offsets[i] + row;  // insert left row index
//                joined_right_indices[num_joined_indices] = key_value_pair->second; // insert right row index
//                ++num_joined_indices;
//            }
//        }

//        auto l = left_joined.array()->GetValues<uint64_t>(1, 0);
//        auto r = right_joined.array()->GetValues<uint64_t>(1, 0);
//
//        new_left_indices_vector[i] = std::vector<uint64_t>(l, l+left_joined.length());
//        new_right_indices_vector[i] = std::vector<uint64_t>(r, r+right_joined.length());

        new_left_indices_vector[i] = std::vector<uint64_t>(joined_left_indices, joined_left_indices + num_joined_indices);
        new_right_indices_vector[i] = std::vector<uint64_t>(joined_right_indices, joined_right_indices + num_joined_indices);
    }
}

void Join::probe_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, Task *ctx) {

    new_left_indices_vector.resize(probe_col->num_chunks());
    new_right_indices_vector.resize(probe_col->num_chunks());

    // Precompute row offsets. A multithreaded probe phase requires that we know
    // all offsets beforehand.
    std::vector<uint64_t> chunk_row_offsets(probe_col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < probe_col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + probe_col->chunk(i - 1)->length();
    }

    int batch_size = probe_col->num_chunks() / 8;
    if (batch_size == 0) batch_size = probe_col->num_chunks();
    int num_batches = probe_col->num_chunks() / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
    if (num_batches == 0) num_batches = 1;

    // Probe phase
    for (int batch_i=0; batch_i<num_batches; batch_i++) {

        // Each task probes one chunk
        ctx->spawnLambdaTask([this, batch_i, batch_size, probe_col, chunk_row_offsets] {
            probe_hash_table_block(probe_col, batch_i, batch_size, chunk_row_offsets);
        });
    }
}

void Join::finish_probe(Task* ctx) {

    int num_indices = 0;
    for (auto& vec : new_left_indices_vector) {
        num_indices += vec.size();
    }

    ctx->spawnLambdaTask([this, num_indices] {
        arrow::Status status;

        arrow::UInt64Builder new_left_indices_builder;
        std::shared_ptr<arrow::UInt64Array> new_left_indices;

        status = new_left_indices_builder.Reserve(num_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // TODO(nicholas): Use UnsafeAppend or index access
        // Append all of the indices to an ArrayBuilder.
        for (int i = 0; i < new_left_indices_vector.size(); i++) {
            status = new_left_indices_builder.AppendValues(
                new_left_indices_vector[i]);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }

        status = new_left_indices_builder.Finish(&new_left_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        joined_indices_[0] = (arrow::Datum(new_left_indices));
    });
    ctx->spawnLambdaTask([this, num_indices] {
        arrow::Status status;

        arrow::UInt64Builder new_right_indices_builder;
        std::shared_ptr<arrow::UInt64Array> new_right_indices;

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
Join::back_propogate_result(const LazyTable& left, LazyTable right,
                            const std::vector<arrow::Datum>& joined_indices) {

    arrow::Status status;
    arrow::Datum new_indices;
    std::vector<LazyTable> output_lazy_tables;

    // The indices of the indices that were joined
    auto left_indices_of_indices = joined_indices_[0];
    auto right_indices_of_indices = joined_indices_[1];

    // Assume that indices are correct and that boundschecking is unecessary.
    // CHANGE TO TRUE IF YOU ARE DEBUGGING
    arrow::compute::TakeOptions take_options(false);

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
    output_lazy_tables.emplace_back(left.table, left.filter, new_indices);


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
    output_lazy_tables.emplace_back(right.table, right.filter, new_indices);

    // Propogate the join to the other tables in the previous OperatorResult.
    // This elimates tuples from other tables in the previous result that were
    // eliminated in the most recent join.
    for (auto &lazy_table : prev_result_->lazy_tables_) {
        if (lazy_table.table != left.table &&
            lazy_table.table != right.table) {
            if (lazy_table.indices.kind() != arrow::Datum::NONE) {

                status = arrow::compute::Take(lazy_table.indices, left_indices_of_indices, take_options).Value(&new_indices);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                output_lazy_tables.emplace_back(
                    lazy_table.table, lazy_table.filter, new_indices);
            } else {
                output_lazy_tables.emplace_back(
                    lazy_table.table, lazy_table.filter, lazy_table.indices);
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
            build_hash_table(right_join_col_.chunked_array(), internal);
        }),
        CreateLambdaTask([this, i](Task *internal) {
            // Probe phase
            probe_hash_table(left_join_col_.chunked_array(), internal);
        }),
        CreateLambdaTask([this, i](Task *internal) {
            finish_probe(internal);
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
    std::vector<Task *> tasks;

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

        // The previous result prev may contain many LazyTables. Find the
        // LazyTables that we want to join.
        for (int i = 0; i < prev_result_->lazy_tables_.size(); i++) {
            auto lazy_table = prev_result_->get_table(i);

            if (left_ref.table == lazy_table.table) {
                lefts.push_back(lazy_table);
            } else if (right_ref.table == lazy_table.table) {
                rights.push_back(lazy_table);
            }
        }
    }

    // Each task is one join
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
    ctx->spawnTask(CreateTaskChain(tasks));
}

void Join::finish() {
    // Must append to output_result_ first
    output_result_->append(prev_result_);
//    prev_result_->append(prev_result_);

}

} // namespace hustle