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

Join::Join(ColumnReference left,
         std::shared_ptr<OperatorResult> left_selection,
         ColumnReference right,
         std::shared_ptr<OperatorResult> right_selection){

    left_join_col_name_ = left.col_name;
    right_join_col_name_ = right.col_name;

    left_table_ = left.table;
    right_table_ = right.table;

    switch(left_selection->get_type()) {
        case SELECT: {
            left_selection_ = std::static_pointer_cast<SelectResult>
                    (left_selection)->filter_;
            break;
        }
        case JOIN: {
            left_join_result_ = std::static_pointer_cast<JoinResult>
                    (left_selection)->join_results_;
            break;
        }
        default: {
            std::cerr << "left_selection must be of SELECT or JOIN type." <<
            std::endl;
        }
    }

    switch(right_selection->get_type()) {
        case SELECT: {
            right_selection_ = std::static_pointer_cast<SelectResult>
                    (right_selection)->filter_;
            break;
        }
        default: {
            std::cerr << "right_selection must be of SELECT type." << std::endl;
        }
    }
}

std::vector<JoinResultColumn> Join::hash_join() {

    std::vector<JoinResultColumn> out_join_result;

    if (left_table_ == nullptr) {
        out_join_result = hash_join(left_join_result_, right_table_);
    }
    else {
        out_join_result = hash_join(left_table_, right_table_);
    }

    return out_join_result;

}

std::vector<JoinResultColumn> Join::hash_join(
        const std::shared_ptr<Table>& left_table,
        const std::shared_ptr<Table>& right_table) {

    arrow::Status status;

    if (left_selection_.is_arraylike()) {
        left_filter_ = left_selection_.chunked_array();
    }
    if (right_selection_.is_arraylike()) {
        right_filter_ = right_selection_.chunked_array();
    }

    auto right_join_col = apply_selection(
            right_table_->get_column_by_name(right_join_col_name_),
            right_selection_);

    right_join_col_ = right_join_col;
    hash_table_ = build_hash_table(right_join_col);

    auto left_join_col = apply_selection(
            left_table_->get_column_by_name(left_join_col_name_),
            left_selection_);
    left_join_col_ = left_join_col;
    auto out = probe_hash_table(left_join_col);

    std::make_shared<JoinResult>(out);
    return out;
}


std::vector<JoinResultColumn> Join::hash_join(
        std::vector<JoinResultColumn>& left_join_result,
        const std::shared_ptr<Table>& right_table) {

    arrow::Status status;

    right_table_ = right_table;

    if (right_selection_.is_arraylike()) {
        right_filter_ = right_selection_.chunked_array();
    }

    auto right_join_col = apply_selection(
            right_table->get_column_by_name(right_join_col_name_),
            right_selection_);


    right_join_col_ = right_join_col;
//right_join_col_ = right_table->get_column_by_name(right_join_col_name_);
    hash_table_ = build_hash_table(right_join_col);

    int selection_reference_index = -1;
    int left_join_col_index = -1;

    // TODO(nicholas): This will find the wrong table if lineorder is not
    //  at index 0, since the left and right join columns usually have the
    //  same name!
    for (int i=0; i<left_join_result_.size(); i++) {
        int index = left_join_result_[i].table->get_schema()->GetFieldIndex
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
    left_table_ = left_join_result_[selection_reference_index].table;
    auto left_join_col = apply_selection(
            left_table_->get_column(left_join_col_index),
            arrow::compute::Datum(left_join_result_[selection_reference_index]
            .filter)
    );
    left_join_col = apply_selection(
            left_table_->get_column(left_join_col_index),
            left_join_result_[selection_reference_index].selection
    );

    left_join_col_ = left_join_col;
//    left_join_col_ = left_table_->get_column(left_join_col_index);

    auto out = probe_hash_table(left_join_col);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    arrow::compute::Datum matched_indices;
    arrow::compute::Datum out_indices;
    std::shared_ptr<arrow::ChunkedArray> out_ref;

    out_indices = arrow::compute::Datum(out[0].selection);

    status = arrow::compute::Match(&function_context, out[0].selection,
            left_join_result_[selection_reference_index].selection,
            &matched_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    status = arrow::compute::Take(&function_context,
                                  left_join_result_[selection_reference_index].selection,
                                  matched_indices,
                                  take_options,
                                  &out[0].selection);

    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    arrow::compute::Datum res;

    std::vector<JoinResultColumn> output;
    output.push_back(out[0]);

    for (int i=0; i<left_join_result_.size(); i++) {
        if (i != selection_reference_index) {

            status = arrow::compute::Take(&function_context,
                                          left_join_result_[i].selection,
                                          matched_indices,
                                          take_options,
                                          &res);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            output.push_back({left_join_result_[i].table,
                              left_join_result_[i].filter,
                              res});

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

std::vector<JoinResultColumn> Join::probe_hash_table
(std::shared_ptr<arrow::ChunkedArray> probe_col) {

    arrow::Status status;

    arrow::Int64Builder left_indices_builder;
    arrow::Int64Builder right_indices_builder;
    std::shared_ptr<arrow::Int64Array> left_indices;
    std::shared_ptr<arrow::Int64Array> right_indices;

    std::shared_ptr<arrow::Int64Array> old_indices;
    if (!left_join_result_.empty()){
        old_indices = std::static_pointer_cast<arrow::Int64Array>
                (left_join_result_[0].selection.make_array());
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

                int left_row_index = -1;
                if (!left_join_result_.empty()) {
                    left_row_index = old_indices->Value(row_offset + row);
                }
                else {
                    left_row_index = row_offset + row;
                }
                status = left_indices_builder.Append(left_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                status = right_indices_builder.Append(right_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }
        row_offset += left_join_chunk->length();
    }

    // Note that ArrayBuilders are automatically reset by default after
    // calling Finish()
    status = left_indices_builder.Finish(&left_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    status = right_indices_builder.Finish(&right_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    left_indices_ = left_indices;
    right_indices_ = right_indices;

    std::vector<JoinResultColumn> out;
    out.push_back({left_table_,
                   arrow::compute::Datum(left_filter_),
                   arrow::compute::Datum(left_indices_)});
    out.push_back({right_table_,
                   arrow::compute::Datum(right_filter_),
                   arrow::compute::Datum(right_indices_)});

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

        std::vector<JoinResultColumn> out_join_result_cols;

        if (!left_join_result_.empty()) {
            out_join_result_cols = hash_join(left_join_result_, right_table_);
        }
        else {
            out_join_result_cols = hash_join(left_table_, right_table_);
        }

        return std::make_shared<JoinResult>(out_join_result_cols);
    }

} // namespace operators
} // namespace hustle
