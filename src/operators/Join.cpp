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

Join::Join(std::shared_ptr<OperatorResult> prev,
               JoinGraph graph) {

    prev_result_ = std::move(prev);
    graph_ = std::move(graph);

}

std::unordered_map<int64_t, int64_t> Join::build_hash_table
(const std::shared_ptr<arrow::ChunkedArray>& col) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    std::unordered_map<int64_t, int64_t> hash(col->length());

    int row_offset = 0;

    for (int i=0; i<col->num_chunks(); i++) {
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                col->chunk(i));

        for (int row=0; row<chunk->length(); row++) {
            hash[chunk->Value(row)] = row_offset + row;
        }
        row_offset += chunk->length();
    }

    return hash;
}

std::vector<arrow::compute::Datum> Join::probe_hash_table
(const std::shared_ptr<arrow::ChunkedArray>& probe_col) {

    arrow::Status status;

    arrow::Int64Builder new_left_indices_builder;
    arrow::Int64Builder new_right_indices_builder;
    std::shared_ptr<arrow::Int64Array> new_left_indices;
    std::shared_ptr<arrow::Int64Array> new_right_indices;

    // Probe phase
    int row_offset = 0;
    for (int i = 0; i < probe_col->num_chunks(); i++) {

        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto left_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                probe_col->chunk(i));

        for (int row = 0; row < left_join_chunk->length(); row++) {
            auto key = left_join_chunk->Value(row);

            if (hash_table_.count(key)) {
                int64_t right_row_index = hash_table_[key];
                int64_t left_row_index = row_offset + row;

                status = new_left_indices_builder.Append(left_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                status = new_right_indices_builder.Append(right_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }
        row_offset += left_join_chunk->length();
    }

    // Note that ArrayBuilders are automatically reset by default after
    // calling Finish()
    status = new_left_indices_builder.Finish(&new_left_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    status = new_right_indices_builder.Finish(&new_right_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    std::vector<arrow::compute::Datum> joined_indices;
    joined_indices.emplace_back(arrow::compute::Datum(new_left_indices));
    joined_indices.emplace_back(arrow::compute::Datum(new_right_indices));

    return joined_indices;
}

std::shared_ptr<OperatorResult> Join::back_propogate_result(
        std::vector<arrow::compute::Datum> joined_indices) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    // Where we will store the result of calls to Take. This will be reused.
    arrow::compute::Datum new_indices;

    std::vector<LazyTable> output_lazy_tables;

    // The indices of the indices that were joined
    auto left_indices_of_indices = joined_indices[0];
    auto right_indices_of_indices = joined_indices[1];

    // Update the indices of the left LazyTable. If there was no previous
    // join on the left table, then left_indices_of_indices directly
    // corresponds to indices in the left table, and we do not need to
    // call Take.
    if (left_.indices.kind() != arrow::compute::Datum::NONE) {
        status = arrow::compute::Take(&function_context,
                                      left_.indices,
                                      left_indices_of_indices,
                                      take_options,
                                      &new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        output_lazy_tables.emplace_back(left_.table,left_.filter, new_indices);
    }
    else {
        new_indices = left_indices_of_indices;
    }
    output_lazy_tables.emplace_back(left_.table,left_.filter, new_indices);


    // Update the indices of the right LazyTable. If there was no previous
    // join on the right table, then right_indices_of_indices directly
    // corresponds to indices in the right table, and we do not need to
    // call Take.
    if (right_.indices.kind() != arrow::compute::Datum::NONE) {
        status = arrow::compute::Take(&function_context,
                                      right_.indices,
                                      right_indices_of_indices,
                                      take_options,
                                      &new_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }
    else {
        new_indices = right_indices_of_indices;
    }
    output_lazy_tables.emplace_back(right_.table,right_.filter, new_indices);


    // Propogate the join to the other tables in the previous
    // OperatorResult.
    for (auto &lazy_table : prev_result_->lazy_tables_) {
        if (lazy_table.table != left_.table &&
            lazy_table.table != right_.table) {
            if (lazy_table.indices.kind() != arrow::compute::Datum::NONE) {

                status = arrow::compute::Take(&function_context,
                                              lazy_table.indices,
                                              left_indices_of_indices,
                                              take_options,
                                              &new_indices);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                output_lazy_tables.emplace_back(lazy_table.table,
                                                lazy_table.filter,
                                                new_indices);
            }
            else {
                output_lazy_tables.emplace_back(lazy_table.table,
                                                lazy_table.filter,
                                                lazy_table.indices);
            }
        }
    }
    return std::make_shared<OperatorResult>(output_lazy_tables);
}

std::shared_ptr<OperatorResult> Join::hash_join() {
    // Build phase
    auto right_join_col = right_.get_column_by_name(right_join_col_name_);
    hash_table_ = build_hash_table(right_join_col);

    // Probe phase
    int left_join_col_index = left_.table->get_schema()->GetFieldIndex
            (left_join_col_name_);
    auto left_join_col = left_.get_column(left_join_col_index);
    auto joined_indices = probe_hash_table(left_join_col);

    // Update indices of other LazyTables in the previous OperatorResult
    return back_propogate_result(joined_indices);
}

std::shared_ptr<OperatorResult> Join::run() {

    arrow::Status status;

    //TODO(nicholas): For now, we assume joins have simple predicates
    // without connective operators.
    // TODO(nicholas): Loop over tables in adj
    auto predicates = graph_.get_predicates(0);

    for (auto &jpred : predicates) {

        auto left = jpred.left_col_ref_;
        auto right = jpred.right_col_ref_;

        left_join_col_name_ = left.col_name;
        right_join_col_name_ = right.col_name;

        // The previous result prev may contain many LazyTables. Find the
        // LazyTables that we want to join.
        for (int i=0; i<prev_result_->lazy_tables_.size(); i++) {
            auto lazy_table = prev_result_->get_table(i);

            if (left.table == lazy_table.table) {
                left_ = lazy_table;
            }
            else if (right.table == lazy_table.table) {
                right_ = lazy_table;
            }
        }

        prev_result_ = hash_join();
    }
    return prev_result_;

}
} // namespace operators
} // namespace hustle
