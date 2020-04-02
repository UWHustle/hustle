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

Join::Join(std::string left_column_name, std::string right_column_name) {
    left_join_column_name_ = std::move(left_column_name);
    right_join_column_name_ = std::move(right_column_name);

    std::tuple<std::shared_ptr<Table>, std::shared_ptr<Table>> tables;

}

std::vector<SelectionReference> Join::hash_join(
        const std::shared_ptr<Table>& left_table,
        const arrow::compute::Datum& left_selection,
        const std::shared_ptr<Table>& right_table,
        const arrow::compute::Datum& right_selection) {

    arrow::Status status;

    left_table_ = left_table;
    right_table_ = right_table;

    if (left_selection.is_arraylike()) {
        left_filter_ = left_selection.chunked_array();
    }
    if (right_selection.is_arraylike()) {
        right_filter_ = right_selection.chunked_array();
    }

    auto right_join_col = apply_selection(
            right_table->get_column_by_name(right_join_column_name_),
            right_selection);

    right_join_col_ = right_join_col;
    hash_table_ = build_hash_table(right_join_col);
//    probe_hash_table(left_table, left_selection);

    auto left_join_col = apply_selection(
            left_table->get_column_by_name(left_join_column_name_),
            left_selection);
    left_join_col_ = left_join_col;
    auto out = probe_hash_table(left_join_col);

    std::cout << "lineorder " << out[0].selection.make_array()->ToString() <<
    std::endl;
    std::cout << "part" << out[1].selection.make_array()->ToString() <<
    std::endl;

    return out;

}


std::vector<SelectionReference> Join::hash_join(
        const std::vector<SelectionReference>& left,
        const std::shared_ptr<Table>& right_table,
        const arrow::compute::Datum& right_selection) {

    arrow::Status status;

    right_table_ = right_table;

    if (right_selection.is_arraylike()) {
        right_filter_ = right_selection.chunked_array();
    }

    auto right_join_col = apply_selection(
            right_table->get_column_by_name(right_join_column_name_),
            right_selection);


    right_join_col_ = right_join_col;
//right_join_col_ = right_table->get_column_by_name(right_join_column_name_);
    hash_table_ = build_hash_table(right_join_col);

    int selection_reference_index = -1;
    int left_join_col_index = -1;

    for (int i=0; i<left.size(); i++) {
        int index = left[i].table->get_schema()->GetFieldIndex
                (left_join_column_name_);
        if (index >= 0) {
            left_join_col_index = index;
            selection_reference_index = i;
            break;
        }
    }
    //TODO(nicholas): left_table_ must also be filtered!
    left_table_ = left[selection_reference_index].table;
    auto left_join_col = apply_selection(
            left_table_->get_column(left_join_col_index),
            left[selection_reference_index].selection
    );

    left_join_col_ = left_join_col;
//    left_join_col_ = left_table_->get_column(left_join_col_index);

    auto out = probe_hash_table(left_join_col);

    std::cout << "lineorder 2 " << out[0].selection.make_array()->ToString() <<
              std::endl;
    std::cout << "part 2" << out[1].selection.make_array()->ToString() <<
              std::endl;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    arrow::compute::Datum matched_indices;
    arrow::compute::Datum out_indices;
    std::shared_ptr<arrow::ChunkedArray> out_ref;

    std::cout << "BEFORE MATCH 1" << out[0].selection.make_array()->ToString
    () <<
    std::endl;
        std::cout << "BEFORE MATCH 2" <<left[selection_reference_index]
        .selection
    .make_array()
    ->ToString() <<std::endl;

        out_indices = arrow::compute::Datum(out[0].selection)
;
//    status = arrow::compute::Match(
//            &function_context,
//           out[0].selection,
//           left[selection_reference_index].selection,
//            &matched_indices);


//    std::cout << "match" << out_indices.make_array()->ToString() <<
//              std::endl;
//    std::cout << "lineorder after" << out[0].selection.make_array()->ToString
//    () <<
//    std::endl;



    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    arrow::compute::Datum res;

    std::vector<SelectionReference> output;
    output.push_back(out[0]);



    for (int i=0; i<left.size(); i++) {
        if (i != selection_reference_index) {

            std::cout << "BEFORE " << left[i].selection.make_array()->ToString
            () <<
                      std::endl;
            std::cout << "TAKING " << out_indices.make_array()->ToString
            () <<
                      std::endl;
            status = arrow::compute::Take(&function_context,
                                          left[i].selection,
                                          out_indices,
                                          take_options,
                                          &res);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            output.push_back({left[i].table, left[i].col, left[i].filter, res});

            std::cout << "AFTER " << res.make_array()->ToString
                    () <<
                      std::endl;
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

std::vector<SelectionReference> Join::probe_hash_table
(std::shared_ptr<arrow::ChunkedArray> probe_col) {

    arrow::Status status;

    arrow::Int64Builder left_indices_builder;
    arrow::Int64Builder right_indices_builder;
    std::shared_ptr<arrow::Int64Array> left_indices;
    std::shared_ptr<arrow::Int64Array> right_indices;

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

                int left_row_index = row_offset + row;
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

    std::vector<SelectionReference> out;
    out.push_back({left_table_, probe_col, left_filter_,
                   get_left_indices()});
    out.push_back({right_table_, right_join_col_, right_filter_,
                   get_right_indices()});

    return out;
}

std::shared_ptr<arrow::ChunkedArray> Join::apply_selection
(std::shared_ptr<arrow::ChunkedArray> col, arrow::compute::Datum selection) {

    arrow::Status status;
    auto out_col = col;
    // If a selection was previously performed on the left table,
    // apply it to the join column.
    if (selection.is_arraylike()) {
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::shared_ptr<arrow::ChunkedArray> out;

        switch(selection.type()->id()) {
            case arrow::Type::BOOL: {
                status = arrow::compute::Filter(&function_context,
                                                *col,
                                                *selection.chunked_array(),
                                                &out_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
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

    std::shared_ptr<Table> Join::run_operator(
            std::vector<std::shared_ptr<Table>> tables) {
        return nullptr;
    }

    arrow::compute::Datum Join::get_indices_for_table(
            const std::shared_ptr<Table>& other) {
        if (other == left_table_) {
            return get_left_indices();
        }
        else {
            return get_right_indices();
        }
    }

    arrow::compute::Datum Join::get_left_indices() {
        arrow::compute::Datum out(left_indices_);
        return out;
    }

    arrow::compute::Datum Join::get_right_indices() {
        arrow::compute::Datum out(right_indices_);
        return out;
    }

} // namespace operators
} // namespace hustle
