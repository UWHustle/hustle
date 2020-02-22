#include "Join.h"

#include <utility>
#include <arrow/compute/api.h>
#include <arrow/compute/kernels/compare.h>
#include <table/util.h>
#include <iostream>
#include <arrow/scalar.h>

#define BLOCK_SIZE 1024

namespace hustle {
namespace operators {

Join::Join(std::string column_name) {
    column_name_ = std::move(column_name);
}

    std::vector<std::shared_ptr<Block>> Join::runOperator(
            std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
        return std::vector<std::shared_ptr<Block>>();
    }

    std::shared_ptr<Table> Join::hash_join(std::shared_ptr<Table> left_table, std::shared_ptr<Table>
        right_table) {

    arrow::Status status;
    std::unordered_map<int64_t, record_id> hash(right_table->get_num_rows());
    std::vector<std::shared_ptr<Block>> out_blocks;
    auto out_table = std::make_shared<Table>("out",left_table->get_schema(),
            BLOCK_SIZE);
    int out_block_counter = 0;
    // Build phase
    for (int i=0; i<right_table->get_num_blocks(); i++) {

        auto right_block = right_table->get_block(i);
        auto join_col = right_block->get_column_by_name(column_name_);
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto join_col_casted = std::static_pointer_cast<arrow::Int64Array>(
                join_col);

        for (int row=0; row<right_block->get_num_rows(); row++) {
            record_id rid = {right_block->get_id(), row};
            hash[join_col_casted->Value(row)] = rid;
        }
    }

    auto test = out_table->get_schema()->fields();

    arrow::Int64Builder left_indices_builder;
    std::shared_ptr<arrow::Int64Array> left_indices;

    std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

    // Probe phase
    for (int i=0; i<left_table->get_num_blocks(); i++) {

        auto left_block = left_table->get_block(i);
        auto join_col = left_block->get_column_by_name(column_name_);
        // TODO(nicholas): for now, we assume the join column is INT64 type.
        auto join_col_casted = std::static_pointer_cast<arrow::Int64Array>(
                join_col);


        for (int row=0; row<left_block->get_num_rows(); row++) {
            auto key = join_col_casted->Value(row);
            if (hash.count(key)) {
                record_id rid = hash[key];
                status = left_indices_builder.Append(row);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            }
        }


        // Note that ArrayBuilders are automatically reset by default after
        // calling Finish()
        status = left_indices_builder.Finish(&left_indices);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        // If no tuples will be outputted, do not create a new block.
        if (left_indices->length() > 0) {
            arrow::compute::FunctionContext function_context(
                    arrow::default_memory_pool());
            arrow::compute::TakeOptions take_options;
            auto *left_col = new arrow::compute::Datum();

            // Skip valid column; valid column is added
            for (int k = 0; k < left_block->get_schema()->num_fields(); k++) {
                status = arrow::compute::Take(&function_context,
                                              left_block->get_column(k),
                                              left_indices, take_options,
                                              left_col);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                out_block_data.push_back(left_col->array());
            }


            auto out_block = std::make_shared<Block>(out_block_counter,
                                                     left_block->get_schema(),
                                                     BLOCK_SIZE);
            out_block->insert_records(out_block_data);
            out_blocks.push_back(out_block);
            out_block_data.clear();
        }
    }

    out_table->add_blocks(out_blocks);
    return out_table;
}

    std::vector<std::shared_ptr<Block>>
    Join::runOperator(std::shared_ptr<arrow::Schema> out_schema,
                      arrow::compute::Datum left_join_val,
                      std::shared_ptr<Table> right) {
        return std::vector<std::shared_ptr<Block>>();
    }


// TODO(nicholas): This entire function can be ignored.
//std::vector<std::shared_ptr<Block>> Join::runOperator(
//        std::shared_ptr<arrow::Schema> out_schema,
//        arrow::compute::Datum left_join_val, std::shared_ptr<Table>
//                right_table) {
//
//    arrow::Status status;
//    std::vector<std::shared_ptr<arrow::ArrayData>> out_record_data;
//
//    for (int right_block_i=0; right_block_i<right_table->get_num_blocks();
//    right_block_i++) {
//
//        auto right_block = right_table->get_block(right_block_i);
//        arrow::compute::FunctionContext function_context(
//                arrow::default_memory_pool());
//        arrow::compute::CompareOptions compare_options(
//                arrow::compute::CompareOperator::EQUAL);
//        auto *filter = new arrow::compute::Datum();
//        // computation
//        status = arrow::compute::Compare(
//                &function_context,
//                left_join_val,
//                right_block->get_records()->GetColumnByName(column_name_),
//                compare_options,
//                filter
//        );
//
//        auto out_block = std::make_shared<Block>(rand(), out_schema,
//                BLOCK_SIZE);
//
//        // TODO(nicholas): Need to implement get_schema() or
//        //  get_field() in Block.
//        for (int col_index = 0;
//             col_index < right_block->get_num_rows(); col_index++) {
//            if (right_block->get_records()->schema()->field(
//                    col_index)->name() !=
//                column_name_) {
//                auto *out_data = new arrow::compute::Datum();
//                status = arrow::compute::Filter(
//                        &function_context,
//                        right_block->get_column(col_index),
//                        *filter,
//                        out_data);
//
//                out_record_data.push_back(out_data->array());
//            }
//        }
//        out_block
//
//    }
//
//
//        for (int col_index = 0; col_index <
//                                left->get_records()->num_columns(); col_index++) {
//
//            std::shared_ptr<arrow::Scalar> scalar;
//
//            std::shared_ptr<arrow::Field> field = left->get_records()
//                    ->schema()->field(i);
//
//            switch (field->type()->id()) {
//
//                case arrow::Type::STRING: {
//                    auto column =
//                            std::static_pointer_cast<arrow::StringArray>(
//                                    left->get_column(i));
//                    scalar = std::make_shared<arrow::StringScalar>
//                            (column->GetString(i));
//
//                    break;
//                }
//                case arrow::Type::BOOL: {
//                    auto column =
//                            std::static_pointer_cast<arrow::BooleanArray>(
//                                    left->get_column(i));
//                    scalar = std::make_shared<arrow::BooleanScalar>
//                            (column->Value(i));
//                    break;
//                }
//                case arrow::Type::INT64: {
//                    auto column =
//                            std::static_pointer_cast<arrow::Int64Array>(
//                                    left->get_column(i));
//                    scalar = std::make_shared<arrow::Int64Scalar>
//                            (column->Value(i));
//                    break;
//                }
//                default: {
//                    throw std::logic_error(
//                            std::string(
//                                    "Cannot join table with "
//                                    "unsupported type: ") +
//                            field->type()->ToString());
//                }
//            }
//
//            std::shared_ptr<arrow::Array> left_col;
//            status = arrow::MakeArrayFromScalar(*scalar,
//                                                out_record_data[0]->length,
//                                                &left_col);
//            evaluate_status(status, __FUNCTION__, __LINE__);
//
//            out_record_data.push_back(left_col->data());
//        }
//
////        out_block.insert_records(out_record_data);
//    }
//}
//
//
//
//std::vector<std::shared_ptr<Block>> Join::runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
//
////    auto schema_fields = std::vector<std::shared_ptr<arrow::Field>>();
////    for (int j = 0;
////         j < left->get_records()->schema()->num_fields(); j++) {
////        schema_fields.push_back(
////                left->get_records()->schema()->field(j)->Copy());
////    }
////    for (int j = 0;
////         j < right->get_records()->schema()->num_fields(); j++) {
////        if (right->get_records()->schema()->field(j)->name() !=
////            column_name_) {
////            schema_fields.push_back(
////                    right->get_records()->schema()->field(j)->Copy());
////        }
////    }
//    auto left_table = std::make_shared<Table>("left",
//            block_groups[0][0]->get_schema(), BLOCK_SIZE);
//    auto right_table = std::make_shared<Table>("left",
//                                              block_groups[0][0]->get_schema(), BLOCK_SIZE);
//
//    auto out_table = std::make_shared<Table>("out", left_table->get_schema(),
//            BLOCK_SIZE);
//
//    left_table->add_blocks(block_groups[0]);
//    right_table->add_blocks(block_groups[1]);
//
//    for (int left_block_i=0; left_block_i<left_table->get_num_blocks();
//    left_block_i++) {
//
//        auto left_block = left_table->get_block(left_block_i);
//
//        for (int left_row_i=0; left_row_i<left_block->get_num_rows();
//        left_row_i++) {
//
//            arrow::compute::Datum join_val;
//
//            auto join_val_field = left_block->get_schema()->GetFieldByName(
//                    column_name_);
//            switch (join_val_field->type()->id()) {
//                case arrow::Type::INT64: {
//                    auto casted_col = std::static_pointer_cast<arrow::Int64Array>(
//                            left_block->get_records()->GetColumnByName(
//                                    column_name_)
//                    );
//                    join_val = arrow::compute::Datum(
//                            casted_col->raw_values()[left_row_i]);
//                }
//                    break;
//                default: {
//                    throw std::logic_error(
//                            std::string(
//                                    "Unsupported type for join operation: ") +
//                            join_val_field->type()->ToString());
//                }
//            }
//
//            // TODO(nicholas): we only return rows in the right table that
//            //  would be joined with rows in the left table. Also, note that
//            //  this implementation assumes that block ids are sequential.
//            auto out_blocks = runOperator(right_table->get_schema(), join_val,
//                    right_table);
//            out_table->add_blocks(out_blocks);
//
//        }
//    }
//
//
//}

} // namespace operators
} // namespace hustle
