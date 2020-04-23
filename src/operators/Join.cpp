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

    auto jpred = graph.get_predicates(0)[0];

    auto left = jpred.left_col_ref_;
    auto right = jpred.right_col_ref_;

    left_join_col_name_ = left.col_name;
    right_join_col_name_ = right.col_name;

    prev_result_ = prev;
    
    for (int i=0; i<prev->lazy_tables_.size(); i++) {
        auto lazy_table = prev->get_table(i);

        if (left.table == lazy_table.table) {
            left_ = lazy_table;
        }
        else if (right.table == lazy_table.table) {
            right_ = lazy_table;
        }
    }

}



std::unordered_map<int64_t, int64_t> Join::build_hash_table
(std::shared_ptr<arrow::ChunkedArray> col) {

    arrow::Status status;
    // TODO(nicholas): size of hash table is too large if selection is not null
    std::unordered_map<int64_t, int64_t> hash(col->length());

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());

    int row_offset = 0;
    // Build phase if there is no selection
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

std::vector<LazyTable> Join::probe_hash_table
(std::shared_ptr<arrow::ChunkedArray> probe_col) {

    arrow::Status status;

    arrow::Int64Builder new_left_indices_builder;
    arrow::Int64Builder new_right_indices_builder;
    std::shared_ptr<arrow::Int64Array> new_left_indices;
    std::shared_ptr<arrow::Int64Array> new_right_indices;

    std::shared_ptr<arrow::Int64Array> old_indices;
    
    if (left_.indices.kind() != arrow::compute::Datum::NONE){
        old_indices = std::static_pointer_cast<arrow::Int64Array>
                (left_.indices.make_array());
    }


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


    std::vector<LazyTable> out;
    out.emplace_back(left_.table,
                   arrow::compute::Datum(left_.filter),
                   arrow::compute::Datum(new_left_indices));
    out.emplace_back(right_.table,
                   arrow::compute::Datum(right_.filter),
                   arrow::compute::Datum(new_right_indices));

    return out;
}


    std::shared_ptr<OperatorResult> Join::run() {

        arrow::Status status;

        // Build phase
        auto right_join_col = right_.get_column_by_name(right_join_col_name_);
        hash_table_ = build_hash_table(right_join_col);

        // Probe phase
        int left_join_col_index = left_.table->get_schema()->GetFieldIndex
                (left_join_col_name_);
        auto left_join_col = left_.get_column(left_join_col_index);
        auto out = probe_hash_table(left_join_col);


        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        arrow::compute::TakeOptions take_options;
        arrow::compute::Datum out_indices;

        out_indices = arrow::compute::Datum(out[0].indices);

        if (left_.indices.kind() != arrow::compute::Datum::NONE) {
            status = arrow::compute::Take(&function_context,
                                          left_.indices,
                                          out_indices,
                                          take_options,
                                          &out[0].indices);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }


        arrow::compute::Datum res;

        std::vector<LazyTable> output;
        output.push_back(out[0]);

        for (auto &lazy_table : prev_result_->lazy_tables_) {
            if (lazy_table.table != left_.table) {
                if (lazy_table.indices.kind() != arrow::compute::Datum::NONE) {
                    status = arrow::compute::Take(&function_context,
                                                  lazy_table.indices,
                                                  out_indices,
                                                  take_options,
                                                  &res);
                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                    output.push_back({lazy_table.table,
                                      lazy_table.filter,
                                      res});
                }
            }
        }

        output.push_back(out[1]);
        auto result = std::make_shared<OperatorResult>(output);

        return result;
    }




} // namespace operators
} // namespace hustle
