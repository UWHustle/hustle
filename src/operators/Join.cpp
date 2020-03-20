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



std::shared_ptr<Table> Join::hash_join(std::shared_ptr<Table> left_table, std::shared_ptr<Table>
    right_table) {

    arrow::Status status;

    arrow::SchemaBuilder schema_builder;
    status = schema_builder.AddSchema(left_table->get_schema());
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    for (auto &field : right_table->get_schema()->fields()) {
        // Exclude extra join column and the right table's valid column
        if (field->name() != right_join_column_name_) {
            status = schema_builder.AddField(field);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }
    }

    status = schema_builder.Finish().status();
    evaluate_status(status,__PRETTY_FUNCTION__, __LINE__);
    auto out_schema = schema_builder.Finish().ValueOrDie();

    auto out_table = std::make_shared<Table>("out", out_schema, BLOCK_SIZE);

    std::unordered_map<int64_t, record_id> hash(right_table->get_num_rows());

    std::vector<std::shared_ptr<Block>> out_blocks;

    int out_block_counter = 0;

    // Build phase
    for (int i=0; i<right_table->get_num_blocks(); i++) {

        auto right_block = right_table->get_block(i);
        auto join_col = right_block->get_column_by_name
                (right_join_column_name_);
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto join_col_casted = std::static_pointer_cast<arrow::Int64Array>(
                join_col);

        for (int row=0; row<right_block->get_num_rows(); row++) {
            // TODO(nicholas): Should I store block_row_offset + row instead?
            record_id rid = {right_block->get_id(), row};
            hash[join_col_casted->Value(row)] = rid;
        }
    }

    auto test = out_table->get_schema()->fields();

    arrow::Int64Builder left_indices_builder;
    arrow::Int64Builder right_indices_builder;
    // TODO(nicholas): left_indicies can be a ChunkedArray. It is not
    //  necessary to force its values to be contiguous
    std::shared_ptr<arrow::Int64Array> left_indices;
    std::shared_ptr<arrow::Int64Array> right_indices;

    std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;

    // Probe phase
    // TODO(nicholas): Should the probe phase be done all at once or one
    //  block at a time?
    for (int i=0; i<left_table->get_num_blocks(); i++) {

        auto left_block = left_table->get_block(i);
        auto join_col = left_block->get_column_by_name(left_join_column_name_);
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto join_col_casted = std::static_pointer_cast<arrow::Int64Array>(
                join_col);


        for (int row = 0; row < left_block->get_num_rows(); row++) {
            auto key = join_col_casted->Value(row);

            if (hash.count(key)) {
                record_id rid = hash[key];

                int left_row_index = left_table->get_block_row_offset(i) + row;
                status = left_indices_builder.Append(left_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                int right_row_index = right_table->get_block_row_offset(
                        rid.block_id) + rid.row_index;
                status = right_indices_builder.Append(right_row_index);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }
    }

        // Note that ArrayBuilders are automatically reset by default after
        // calling Finish()
        status = left_indices_builder.Finish(&left_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        status = right_indices_builder.Finish(&right_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // If no tuples will be outputted, do not create a new block.
        if (left_indices->length() > 0) {
            arrow::compute::FunctionContext function_context(
                    arrow::default_memory_pool());
            arrow::compute::TakeOptions take_options;
//            auto *left_col = new arrow::compute::Datum();
            std::shared_ptr<arrow::ChunkedArray> left_col;

            for (int k = 0; k < left_table->get_num_cols(); k++) {
                status = arrow::compute::Take(&function_context,
                                              *left_table->get_column(k),
                                              *left_indices, take_options,
                                              &left_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                out_table_data.push_back(left_col);
            }

            std::shared_ptr<arrow::ChunkedArray> right_col;

            for (int k = 0; k < right_table->get_block(0)->get_num_cols(); k++) {
                // Do not duplicate the join column (natural join)
                if (right_table->get_block(0)->get_schema()->field(k)->name() !=
                right_join_column_name_) {
                    status = arrow::compute::Take(&function_context,
                                                  *right_table->get_column(k),
                                                  *right_indices, take_options,
                                                  &right_col);
                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                    out_table_data.push_back(right_col);
                }
            }

            std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

            for (int chunk_i=0; chunk_i<out_table_data[0]->num_chunks();
            chunk_i++) {
                for (auto &col : out_table_data) {
                    out_block_data.push_back(col->chunk(chunk_i)->data());
                }
                out_table->insert_records(out_block_data);
                out_block_data.clear();
            }
        }

    return out_table;
}

//    void Join::set_children(
//            std::shared_ptr<Operator> left_child,
//            std::shared_ptr<Operator> right_child,
//            FilterOperator filter_operator) {
//
//}

    std::shared_ptr<Table> Join::runOperator(
            std::vector<std::shared_ptr<Table>> tables) {
        return nullptr;
    }

} // namespace operators
} // namespace hustle
