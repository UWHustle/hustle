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

    left_table_ = left.table;
    right_table_ = right.table;

    prev_result_ = prev;
    
    for (int i=0; i<prev->lazy_tables_.size(); i++) {
        auto lazy_table = prev->get_table(i);
        if (left_table_ == lazy_table.table) {
            left_filter_ = lazy_table.filter;
            left_indices_ = lazy_table.indices;
            left_ = lazy_table;
        }
        else if (right_table_ == lazy_table.table) {
            right_filter_ = lazy_table.filter;
            right_indices_ = lazy_table.indices;
            right_ = lazy_table;
        }
    }

}


std::vector<LazyTable> Join::hash_join(
        std::vector<LazyTable>& left_join_result,
        const std::shared_ptr<Table>& right_table) {

    arrow::Status status;

    auto right_join_col = right_.get_column_by_name(right_join_col_name_);
    right_join_col_ = right_join_col;

    hash_table_ = build_hash_table(right_join_col);

    int selection_reference_index = -1;
    int left_join_col_index = -1;

    // TODO(nicholas): This will find the wrong table if lineorder is not
    //  at index 0, since the left and right join columns usually have the
    //  same name!
    for (int i=0; i<prev_result_->lazy_tables_.size(); i++) {
        int index = prev_result_->lazy_tables_[i].table->get_schema()->GetFieldIndex
                (left_join_col_name_);
        if (index >= 0) {
            left_join_col_index = index;
            selection_reference_index = i;
            break;
        }
    }
    //TODO(nicholas): left_table_ must also be filtered! None of the SSB
    // queries with multiple joins perform a selection on Lineorder beforehand,
    // so this part is not yet verified.
//    auto left_join_col = left_.get_column_by_name(left_join_col_name_);
    auto left_join_col = left_.get_column(left_join_col_index);

    left_table_ = prev_result_->lazy_tables_[selection_reference_index].table;
    left_join_col_ = left_join_col;


    auto out = probe_hash_table(left_join_col);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    arrow::compute::Datum out_indices;

    out_indices = arrow::compute::Datum(out[0].indices);

    if (prev_result_->lazy_tables_[selection_reference_index].indices.kind() !=
        arrow::compute::Datum::NONE) {
        status = arrow::compute::Take(&function_context,
                                      prev_result_->lazy_tables_[selection_reference_index].indices,
                                      out_indices,
                                      take_options,
                                      &out[0].indices);
    }

    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    arrow::compute::Datum res;

    std::vector<LazyTable> output;
    output.push_back(out[0]);

    // BUG: Should only apply Take on the RIGHT TABLE, not on any of the
    // unused tables.
    for (int i=0; i<prev_result_->lazy_tables_.size(); i++) {
        if (i != selection_reference_index) {
            if (prev_result_->lazy_tables_[i].indices.kind() !=
            arrow::compute::Datum::NONE) {
                status = arrow::compute::Take(&function_context,
                                              prev_result_->lazy_tables_[i].indices,
                                              out_indices,
                                              take_options,
                                              &res);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                output.push_back({prev_result_->lazy_tables_[i].table,
                                  prev_result_->lazy_tables_[i].filter,
                                  res});
            }
        }
    }

    output.push_back(out[1]);

    return output;

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
    
    int index = -1;
    for (int i=0; i<prev_result_->lazy_tables_.size(); i++) {
        if (prev_result_->lazy_tables_[i].table == left_table_) {
            index = i;
        }
    }
    
    if (left_indices_.kind() != arrow::compute::Datum::NONE){
        old_indices = std::static_pointer_cast<arrow::Int64Array>
                (prev_result_->lazy_tables_[index].indices.make_array());
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
    out.emplace_back(left_table_,
                   arrow::compute::Datum(left_filter_),
                   arrow::compute::Datum(new_left_indices));
    out.emplace_back(right_table_,
                   arrow::compute::Datum(right_filter_),
                   arrow::compute::Datum(new_right_indices));

    return out;
}

std::shared_ptr<arrow::ChunkedArray> Join::apply_selection
(std::shared_ptr<arrow::ChunkedArray> col, arrow::compute::Datum selection) {

    arrow::Status status;
    auto datum_col = arrow::compute::Datum(col);
    auto out_col = col;
    // If a selection was previously performed on the left table,
    // apply it to the join column.
    if (selection.is_arraylike()) {
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        arrow::compute::FilterOptions filter_options;
        std::shared_ptr<arrow::ChunkedArray> out;

        switch(selection.type()->id()) {
            case arrow::Type::BOOL: {
                status = arrow::compute::Filter(&function_context,
                                                col,
                                                selection.chunked_array(),
                                                filter_options,
                                                &datum_col);

                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                out_col = datum_col.chunked_array();
                break;
            }
            case arrow::Type::INT64: {
                arrow::compute::TakeOptions take_options;

                status = arrow::compute::Take(&function_context,
                                              *col,
                                              *selection.make_array(),
                                              take_options,
                                              &out_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
        }
    }

    return out_col;
}

    std::shared_ptr<OperatorResult> Join::run() {

        std::vector<LazyTable> out_join_result_cols;

        out_join_result_cols = hash_join(prev_result_->lazy_tables_, right_table_);

        auto result = std::make_shared<OperatorResult>(out_join_result_cols);

        return result;
    }




} // namespace operators
} // namespace hustle
