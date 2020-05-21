#include "Join.h"

#include <utility>
#include <arrow/compute/api.h>
#include <arrow/compute/kernels/compare.h>
#include <arrow/compute/kernels/match.h>
#include <table/util.h>
#include <iostream>
#include <arrow/scalar.h>

namespace hustle {
namespace operators {

Join::Join(
    const std::size_t query_id,
    std::shared_ptr<OperatorResult> prev,
    JoinGraph graph) : Operator(query_id) {

    prev_result_ = std::move(prev);
    graph_ = std::move(graph);
    joined_indices_.resize(2);

}

void Join::build_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &col, Task *ctx) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    hash_table_.reserve(col->length());

    // Precompute the row offsets of each chunk. A multithreaded build phase
    // requires that we know all offsets beforehand.
    std::vector<int64_t> chunk_row_offsets(col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + col->chunk(i - 1)->length();
    }

    for (int i = 0; i < col->num_chunks(); i++) {
        // Each task inserts one chunk into the hash table
        ctx->spawnLambdaTask([this, i, col, chunk_row_offsets] {
            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                col->chunk(i));

            for (int row = 0; row < chunk->length(); row++) {
                std::scoped_lock<std::mutex> lock(hash_table_mutex_);
                hash_table_[chunk->Value(row)] = chunk_row_offsets[i] + row;
            }
        });
    }
}

void Join::probe_hash_table
    (const std::shared_ptr<arrow::ChunkedArray> &probe_col, Task *ctx) {
    // It would make much more sense to use an ArrayVector instead of a vector of
    // vectors, since we can make a ChunkedArray out from an ArrayVector. But
    // Arrow's Take function is very inefficient when the indices are in a
    // ChunkedArray. So, we instead use a vector of vectors to store the indices,
    // and then construct an Array from the vector of vectors.

    // The indices of rows joined in chunk i are stored in
    // new_left_indices_vector[i] and new_right_indices_vector[i]
    new_left_indices_vector.resize(probe_col->num_chunks());
    new_right_indices_vector.resize(probe_col->num_chunks());

    // Precompute row offsets. A multithreaded probe phase requires that we know
    // all offsets beforehand.
    std::vector<int64_t> chunk_row_offsets(probe_col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < probe_col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + probe_col->chunk(i - 1)->length();
    }

    // Probe phase
    for (int i = 0; i < probe_col->num_chunks(); i++) {

        // Each task probes one chunk
        ctx->spawnLambdaTask([this, i, probe_col, chunk_row_offsets] {
            arrow::Status status;
            // The indices of the rows joined in chunk i
            std::vector<int64_t> joined_left_indices;
            std::vector<int64_t> joined_right_indices;

            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto left_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                probe_col->chunk(i));

            int64_t row_offset = chunk_row_offsets[i];

            for (int row = 0; row < left_join_chunk->length(); row++) {
                auto key = left_join_chunk->Value(row);

                if (hash_table_.count(key)) {
                    int64_t right_row_index = hash_table_[key];
                    int64_t left_row_index = row_offset + row;

                    joined_left_indices.push_back(left_row_index);
                    joined_right_indices.push_back(right_row_index);
                }
            }

            new_left_indices_vector[i] = joined_left_indices;
            new_right_indices_vector[i] = joined_right_indices;
        });
    }
}

void Join::finish_probe() {
    arrow::Status status;
    arrow::Int64Builder new_left_indices_builder;
    arrow::Int64Builder new_right_indices_builder;
    std::shared_ptr<arrow::Int64Array> new_left_indices;
    std::shared_ptr<arrow::Int64Array> new_right_indices;

    // Append all of the indices to an ArrayBuilder.
    for (int i = 0; i < new_left_indices_vector.size(); i++) {
        status = new_left_indices_builder.AppendValues(
            new_left_indices_vector[i]);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        status = new_right_indices_builder.AppendValues(
            new_right_indices_vector[i]);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }

    // Construct left and right index arrays.
    status = new_left_indices_builder.Finish(&new_left_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    status = new_right_indices_builder.Finish(&new_right_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    // Store the index arrays.
    joined_indices_[0] = (arrow::compute::Datum(new_left_indices));
    joined_indices_[1] = (arrow::compute::Datum(new_right_indices));
}

std::shared_ptr<OperatorResult>
Join::back_propogate_result(LazyTable left, LazyTable right,
                            std::vector<arrow::compute::Datum> joined_indices) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
        arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    // Where we will store the result of calls to Take. This will be reused.
    arrow::compute::Datum new_indices;

    std::vector<LazyTable> output_lazy_tables;

    // The indices of the indices that were joined
    auto left_indices_of_indices = joined_indices_[0];
    auto right_indices_of_indices = joined_indices_[1];


    // Update the indices of the left LazyTable. If there was no previous
    // join on the left table, then left_indices_of_indices directly
    // corresponds to indices in the left table, and we do not need to
    // call Take.
    if (left.indices.kind() != arrow::compute::Datum::NONE) {
        status = arrow::compute::Take(
            &function_context, left.indices, left_indices_of_indices,
            take_options,
            &new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        output_lazy_tables.emplace_back(left.table, left.filter, new_indices);
    } else {
        new_indices = left_indices_of_indices;
    }
    output_lazy_tables.emplace_back(left.table, left.filter, new_indices);


    // Update the indices of the right LazyTable. If there was no previous
    // join on the right table, then right_indices_of_indices directly
    // corresponds to indices in the right table, and we do not need to
    // call Take.
    if (right.indices.kind() != arrow::compute::Datum::NONE) {
        status = arrow::compute::Take(
            &function_context, right.indices, right_indices_of_indices,
            take_options, &new_indices);
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
            if (lazy_table.indices.kind() != arrow::compute::Datum::NONE) {

                status = arrow::compute::Take(
                    &function_context, lazy_table.indices,
                    left_indices_of_indices,
                    take_options, &new_indices);
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

void Join::hash_join(LazyTable left, std::string left_col, LazyTable right,
                     std::string right_col, Task *ctx) {

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this, right, right_col](Task *internal) {
            // Build phase
            auto right_join_col = right.get_column_by_name(right_col);
            build_hash_table(right_join_col, internal);
        }),
        CreateLambdaTask([this, left, left_col](Task *internal) {
            // Probe phase
            int left_join_col_index = left.table->get_schema()->GetFieldIndex(
                left_col);
            auto left_join_col = left.get_column(left_join_col_index);
            probe_hash_table(left_join_col, internal);
        }),
        CreateLambdaTask([this, left, right](Task *internal) {
            finish_probe();
            // Update indices of other LazyTables in the previous OperatorResult
            prev_result_ = back_propogate_result(left, right, joined_indices_);
        })
    ));
}

void Join::execute(Task *ctx) {

    // To handle a variable number of joins, we must store the tasks beforehand. The variadic CreateTaskChain
    // cannot help us here!
    std::vector<Task *> tasks;

    std::vector<LazyTable> lefts;
    std::vector<LazyTable> rights;
    std::vector<std::string> left_col_names;
    std::vector<std::string> right_col_names;

    // TODO(nicholas): For now, we assume joins have simple predicates
    //   without connective operators.
    // TODO(nicholas): Loop over tables in adj
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
            [=](Task *internal) {
                hash_join(lefts[i], left_col_names[i], rights[i],
                          right_col_names[i],
                          internal);
            }));
    }

    // The (j+1)st task in tasks is not executed until the jth task finishes.
    ctx->spawnTask(CreateTaskChain(tasks));
}

std::shared_ptr<OperatorResult> Join::finish() {
    return prev_result_;
}

} // namespace operators
} // namespace hustle