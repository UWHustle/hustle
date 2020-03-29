#include "Join.h"

#include <utility>
#include <arrow/compute/api.h>
#include <arrow/compute/kernels/compare.h>
#include <table/util.h>
#include <iostream>
#include <arrow/scalar.h>

namespace hustle {
namespace operators {

Join::Join(std::string left_column_name, std::string right_column_name) {
    left_join_column_name_ = std::move(left_column_name);
    right_join_column_name_ = std::move(right_column_name);
}

void Join::hash_join(
        const std::shared_ptr<Table>& left_table,
        const arrow::compute::Datum& left_selection,
        const std::shared_ptr<Table>& right_table,
        const arrow::compute::Datum& right_selection) {

    left_table_ = left_table;
    right_table_ = right_table;

    arrow::Status status;

    arrow::SchemaBuilder schema_builder;
    status = schema_builder.AddSchema(left_table->get_schema());
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    for (auto &field : right_table->get_schema()->fields()) {
        if (field->name() != right_join_column_name_) {
            status = schema_builder.AddField(field);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }
    }

    status = schema_builder.Finish().status();
    evaluate_status(status,__PRETTY_FUNCTION__, __LINE__);
    auto out_schema = schema_builder.Finish().ValueOrDie();

    auto out_table = std::make_shared<Table>("out", out_schema, BLOCK_SIZE);

    std::unordered_map<int64_t, int64_t> hash(right_table->get_num_rows());

    std::vector<std::shared_ptr<Block>> out_blocks;


    auto right_join_col = right_table->get_column_by_name
                (right_join_column_name_);

    // If a selection was previously performed on the right table, apply
    // it to the join column.
    if (right_selection.is_arraylike()) {
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::shared_ptr<arrow::ChunkedArray> out;

        switch(right_selection.type()->id()) {
            // right_selection is a bit filter, i.e. a selection was
            // performed before executing this join
            case arrow::Type::BOOL: {
                // Note: filters will be passed as ChunkedArray Datums
                status = arrow::compute::Filter(&function_context,
                                                *right_join_col,
                                                *right_selection.chunked_array(),
                                                &right_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
            // right_selection is an array of indices, i.e. a join was
            // performed before executing this join.
            case arrow::Type::INT64: {
                arrow::compute::TakeOptions take_options;

                // Note: indices will be passed as Array Datums
                status = arrow::compute::Take(&function_context,
                                                *right_join_col,
                                                *right_selection.make_array(),
                                                take_options,
                                                &right_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
        }
    }

    int block_offset = 0;
    // Build phase
    for (int i=0; i<right_join_col->num_chunks(); i++) {
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto right_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                right_join_col->chunk(i));

        for (int row=0; row<right_join_chunk->length(); row++) {
            hash[right_join_chunk->Value(row)] = block_offset + row;
        }
        block_offset += right_join_chunk->length();
    }

    arrow::Int64Builder left_indices_builder;
    arrow::Int64Builder right_indices_builder;
    std::shared_ptr<arrow::Int64Array> left_indices;
    std::shared_ptr<arrow::Int64Array> right_indices;

    std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;

    auto left_join_col = left_table->get_column_by_name
            (left_join_column_name_);

    // If a selection was previously performed on the left table,
    // apply it to the join column.
    if (left_selection.is_arraylike()) {
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::shared_ptr<arrow::ChunkedArray> out;

        switch(left_selection.type()->id()) {
            case arrow::Type::BOOL: {
                status = arrow::compute::Filter(&function_context,
                                                *left_join_col,
                                                *left_selection.chunked_array(),
                                                &left_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
            case arrow::Type::INT64: {
                arrow::compute::TakeOptions take_options;

                status = arrow::compute::Take(&function_context,
                                              *left_join_col,
                                              *left_selection.make_array(),
                                              take_options,
                                              &left_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                break;
            }
        }
    }


    int left_block_offset = 0;
    // Probe phase for no left_selection
    for (int i = 0; i < left_join_col->num_chunks(); i++) {

        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto left_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                left_join_col->chunk(i));


        for (int row = 0; row < left_join_chunk->length(); row++) {
            auto key = left_join_chunk->Value(row);

            if (hash.count(key)) {
                int64_t right_row_index = hash[key];


                int left_row_index = left_block_offset + row;
                status = left_indices_builder.Append(left_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                status = right_indices_builder.Append(right_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }
        left_block_offset += left_join_chunk->length();
    }

    // Note that ArrayBuilders are automatically reset by default after
    // calling Finish()
    status = left_indices_builder.Finish(&left_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    status = right_indices_builder.Finish(&right_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    left_indices_ = left_indices;
    right_indices_ = right_indices;
}


std::unordered_map<int64_t, int64_t> Join::build_hash_table
(std::shared_ptr<Table> right_table, arrow::compute::Datum right_selection) {

    arrow::Status status;
    // TODO(nicholas): size of hash table is too large if selection is not null
    std::unordered_map<int64_t, int64_t> hash(right_table->get_num_rows());

    auto right_join_col = right_table->get_column_by_name
            (right_join_column_name_);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());

    if (right_selection.is_arraylike()) {
        switch (right_selection.type()->id()) {
            // right_selection is a bit filter, i.e. a selection was
            // performed before executing this join
            case arrow::Type::BOOL: {
                // Note: filters will be passed as ChunkedArray Datums
                status = arrow::compute::Filter(&function_context,
                                                *right_join_col,
                                                *right_selection.chunked_array(),
                                                &right_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                // Build phase after filter selection
                // NOTE: index i corresponds to the row index AFTER filtering.
                int row_offset = 0;

                for (int i = 0; i < right_join_col->num_chunks(); i++) {
                    // TODO(nicholas): for now, we assume the join column is INT64 type.
                    auto right_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                            right_join_col->chunk(i));

                    for (int row = 0; row < right_join_chunk->length(); row++) {
                        hash[right_join_chunk->Value(row)] = row_offset + row;
                        row_offset += right_join_chunk->length();
                        // Hash table stores indices of the FILTERED join col.
                    }
                }
                break;
            }
                // right_selection is an array of indices, i.e. a join was
                // performed before executing this join.
            case arrow::Type::INT64: {
                arrow::compute::TakeOptions take_options;

                // Note: indices will be passed as Array Datums
                status = arrow::compute::Take(&function_context,
                                              *right_join_col,
                                              *right_selection.make_array(),
                                              take_options,
                                              &right_join_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                // Build phase after filter selection
                // NOTE: index i corresponds to the row index AFTER "taking".
                int row_offset = 0;

                for (int i = 0; i < right_join_col->num_chunks(); i++) {
                    // TODO(nicholas): for now, we assume the join column is INT64 type.
                    auto right_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                            right_join_col->chunk(i));

                    for (int row = 0; row < right_join_chunk->length(); row++) {
                        hash[right_join_chunk->Value(row)] = row_offset + row;
                        row_offset += right_join_chunk->length();
                        // Hash table stores indices of the FILTERED join col.
                    }
                }
                break;
            }
        }
    }
    else {
        // Build phase
        for (int i=0; i<right_join_col->num_chunks(); i++) {
            // TODO(nicholas): for now, we assume the join column is INT64 type.
            auto right_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
                    right_join_col->chunk(i));

            for (int row=0; row<right_join_chunk->length(); row++) {
                // TODO(nicholas): Should I store block_row_offset + row instead?
                // index i corresponds to the block id.
//                record_id rid = {i, row};
//                hash[right_join_chunk->Value(row)] = rid;
                // We only need this info if no selection was done beforehand.
            }
        }
    }
}


    std::shared_ptr<Table> Join::hash_join2(
            const std::shared_ptr<Table>& left_table,
            const arrow::compute::Datum& left_selection,
            const std::shared_ptr<Table>& right_table,
            const arrow::compute::Datum& right_selection) {

        left_table_ = left_table;
        right_table_ = right_table;

        arrow::Status status;

        arrow::SchemaBuilder schema_builder;
        status = schema_builder.AddSchema(left_table->get_schema());
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        for (auto &field : right_table->get_schema()->fields()) {
            if (field->name() != right_join_column_name_) {
                status = schema_builder.AddField(field);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }

        status = schema_builder.Finish().status();
        evaluate_status(status,__PRETTY_FUNCTION__, __LINE__);
        auto out_schema = schema_builder.Finish().ValueOrDie();

        auto out_table = std::make_shared<Table>("out", out_schema, BLOCK_SIZE);
//
//
////        std:: cout << right_join_col->chunk(0)->ToString() << std::endl;
//        // BUG: right_selection/left_selection may not be chunked!!!
//        auto left_selection_indices = std::static_pointer_cast
//                <arrow::Int64Array>(left_selection.make_array());
//        auto right_selection_indices = std::static_pointer_cast
//                <arrow::Int64Array>(right_selection.make_array());
//
//        // Build phase
//        int j = 0; // right_selection_indices row index
//        for (int i=0; i<right_join_col->num_chunks(); i++) {
//            // TODO(nicholas): for now, we assume the join column is INT64 type.
//            auto right_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
//                    right_join_col->chunk(i));
//
//            for (int row=0; row<right_join_chunk->length(); row++) {
//                // Note that right_seleciton_indices is not a ChunkedArray, so we
//                // can't use row for both!
//                hash[right_join_chunk->Value(row)] =
//                        right_selection_indices->Value(j++);
//            }
//        }
//
//        arrow::Int64Builder left_indices_builder;
//        arrow::Int64Builder right_indices_builder;
//        // TODO(nicholas): left_indicies can be a ChunkedArray. It is not
//        //  necessary to force its values to be contiguous
//        std::shared_ptr<arrow::Int64Array> left_indices;
//        std::shared_ptr<arrow::Int64Array> right_indices;
//
//        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;
//
//        auto left_join_col = left_table->get_column_by_name
//                (left_join_column_name_);
//
//        switch(left_selection.type()->id()) {
//            case arrow::Type::BOOL: {
//                status = arrow::compute::Filter(&function_context,
//                                                *left_join_col,
//                                                *left_selection.chunked_array(),
//                                                &left_join_col);
//                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                break;
//            }
//            case arrow::Type::INT64: {
//                arrow::compute::TakeOptions take_options;
//
//                status = arrow::compute::Take(&function_context,
//                                              *left_join_col,
//                                              *left_selection.make_array(),
//                                              take_options,
//                                              &left_join_col);
//                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                break;
//            }
//        }
//
//        // Probe phase
//        int k = 0;
//        for (int i = 0; i < left_join_col->num_chunks(); i++) {
//
//            // TODO(nicholas): for now, we assume the join column is INT64 type.
//            auto left_join_chunk = std::static_pointer_cast<arrow::Int64Array>(
//                    left_join_col->chunk(i));
//
//            for (int row = 0; row < left_join_chunk->length(); row++) {
//                auto key = left_join_chunk->Value(row);
//
//                if (hash.count(key)) {
//                    int64_t right_index = hash[key];
//
//                    int left_row_index = left_selection_indices->Value(k);
//                    status = left_indices_builder.Append(left_row_index);
//                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//
//                    int right_row_index = right_selection_indices->Value
//                            (right_index);
//                    status = right_indices_builder.Append(right_row_index);
//                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                }
//                else {
//                    std::cout << key << std::endl;
//                }
//                k++;
//            }
//        }
//
//
//        // Note that ArrayBuilders are automatically reset by default after
//        // calling Finish()
//        status = left_indices_builder.Finish(&left_indices);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//        status = right_indices_builder.Finish(&right_indices);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//
//        left_indices_ = left_indices;
//        right_indices_ = right_indices;
//
////        std::cout << left_indices->ToString() << std::endl;
////        std::cout << right_indices->ToString() << std::endl;
//
//        // If no tuples will be outputted, do not create a new block.
//        if (left_indices->length() > 0) {
//            arrow::compute::FunctionContext function_context(
//                    arrow::default_memory_pool());
//            arrow::compute::TakeOptions take_options;
//            std::shared_ptr<arrow::ChunkedArray> left_out_col;
//
//            for (int k = 0; k < left_table->get_num_cols(); k++) {
//                status = arrow::compute::Take(&function_context,
//                                              *left_table->get_column(k),
//                                              *left_indices, take_options,
//                                              &left_out_col);
//                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                out_table_data.push_back(left_out_col);
//            }
//
//            std::shared_ptr<arrow::ChunkedArray> right_out_col;
//
//            for (int k = 0; k < right_table->get_block(0)->get_num_cols(); k++) {
//                // Do not duplicate the join column (natural join)
//                if (right_table->get_block(0)->get_schema()->field(k)->name() !=
//                    right_join_column_name_) {
//                    status = arrow::compute::Take(&function_context,
//                                                  *right_table->get_column(k),
//                                                  *right_indices, take_options,
//                                                  &right_out_col);
//                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                    out_table_data.push_back(right_out_col);
//                }
//            }
//
//            std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;
//
//            for (int chunk_i=0; chunk_i<out_table_data[0]->num_chunks();
//                 chunk_i++) {
//                for (auto &col : out_table_data) {
//                    out_block_data.push_back(col->chunk(chunk_i)->data());
//                }
//                out_table->insert_records(out_block_data);
//                out_block_data.clear();
//            }
//        }

        return out_table;
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
