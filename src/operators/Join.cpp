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

std::shared_ptr<Block> Join::runOperator(
        std::shared_ptr<Block> left, std::shared_ptr<Block> right) {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayData>> out_record_data;

    auto schema_fields = std::vector<std::shared_ptr<arrow::Field>>();
    for (int j = 0; j < left->get_records()->schema()->num_fields(); j++) {
        schema_fields.push_back(left->get_records()->schema()->field(j)->Copy());
    }
    for (int j = 0; j < right->get_records()->schema()->num_fields(); j++) {
        if (right->get_records()->schema()->field(j)->name() != column_name_) {
            schema_fields.push_back(right->get_records()->schema()->field(j)->Copy());
        }
    }
    auto out_schema = arrow::schema(schema_fields);



    for (int i=0; i< left->get_num_rows(); i++) {

        // Look at the value in the Left Table we want to join with the
        // entire right table
        arrow::compute::Datum join_val;
        auto join_val_field = left->get_records()->schema()->GetFieldByName(column_name_);
        switch (join_val_field->type()->id()) {
            case arrow::Type::INT64: {
                auto casted_col = std::static_pointer_cast<arrow::Int64Array>(
                        left->get_records()->GetColumnByName(column_name_)
                );
                join_val = arrow::compute::Datum(casted_col->raw_values()[i]);
            }
                break;
            default: {
                throw std::logic_error(
                        std::string("Unsupported type for join operation: ") +
                        join_val_field->type()->ToString());
            }
        }



        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        arrow::compute::CompareOptions compare_options(arrow::compute::CompareOperator::EQUAL);
        auto *filter = new arrow::compute::Datum();
        // computation
        status = arrow::compute::Compare(
                &function_context,
                join_val,
                right->get_records()->GetColumnByName(column_name_),
                compare_options,
                filter
        );

        // TODO(nicholas): This is not a natural join; I do not skip the
        //  duplicate join column.
        for (int col_index=0; col_index<right->get_num_rows(); col_index++) {
            auto *out_data = new arrow::compute::Datum();
            status = arrow::compute::Filter(
                    &function_context,
                    right->get_records()->column(col_index),
                    *filter,
                    out_data);
            out_record_data.push_back(out_data->array());
        }

        for (int col_index=0; col_index<left->get_num_rows(); col_index++) {
            auto scalar = std::make_shared<arrow::Int64Scalar>(join_val);
            std::shared_ptr<arrow::Array> left_col;
            arrow::MakeArrayFromScalar(*scalar,
                                       out_record_data[0]->length,
                                       &left_col);

            out_record_data.push_back(left_col->data());
        }

    }

}

std::vector<std::shared_ptr<Block>> Join::runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
  // natural join block_groups[0][x] and block_groups[1][x]
  arrow::Status status;
  auto left_blocks = block_groups[0];
  auto right_blocks = block_groups[1];
  if (left_blocks.size() != right_blocks.size()) {
    // error, block groups not same size
  }
  auto out = std::vector<std::shared_ptr<Block>>();
  for (int i = 0; i < left_blocks.size(); i++) {
    // create schema
    auto schema_fields = std::vector<std::shared_ptr<arrow::Field>>();
    for (int j = 0; j < left_blocks[i]->get_records()->schema()->num_fields(); j++) {
      schema_fields.push_back(left_blocks[i]->get_records()->schema()->field(j)->Copy());
    }
    for (int j = 0; j < right_blocks[i]->get_records()->schema()->num_fields(); j++) {
      if (right_blocks[i]->get_records()->schema()->field(j)->name() != column_name_) {
        schema_fields.push_back(right_blocks[i]->get_records()->schema()->field(j)->Copy());
      }
    }
    auto out_schema = arrow::schema(schema_fields);
    auto out_record_data = std::vector<std::vector<std::shared_ptr<arrow::compute::Datum>>>();
    for (int ii = 0; ii < left_blocks[i]->get_records()->num_columns() + right_blocks[i]->get_records()->num_columns();
         ii++) {
      out_record_data.emplace_back();
    }
    // join
    // outer loop
    for (int i_o = 0; i_o < left_blocks[i]->get_records()->GetColumnByName(column_name_)->length(); i_o++) {
      // extract outer loop value
      arrow::compute::Datum join_val;
      auto join_val_field = left_blocks[i]->get_records()->schema()->GetFieldByName(column_name_);
      switch (join_val_field->type()->id()) {
        case arrow::Type::INT64: {
          auto casted_col = std::static_pointer_cast<arrow::Int64Array>(
              left_blocks[i]->get_records()->GetColumnByName(column_name_)
          );
          join_val = arrow::compute::Datum(casted_col->raw_values()[i_o]);
        }
          break;
        default: {
          throw std::logic_error(
              std::string("Unsupported type for join operation: ") +
                  join_val_field->type()->ToString());
        }
          break;
      }
      arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
      arrow::compute::CompareOptions compare_options(arrow::compute::CompareOperator::EQUAL);
      auto *left_filter = new arrow::compute::Datum();
      // computation
      status = arrow::compute::Compare(
          &function_context,
          join_val,
          right_blocks[i]->get_records()->GetColumnByName(column_name_),
          compare_options,
          left_filter
      );
      auto *right_filter = new arrow::compute::Datum();
      // computation
      status = arrow::compute::Compare(
          &function_context,
          join_val,
          right_blocks[i]->get_records()->GetColumnByName(column_name_),
          compare_options,
          right_filter
      );
      if (!status.ok()) {
        // compare failed, could not generate filter
        evaluate_status(status, __FUNCTION__, __LINE__);
      }
      for (int i_i = 0; i_i < left_blocks[i]->get_records()->num_columns(); i_i++) {
        auto *out_data = new arrow::compute::Datum();
        status =
            arrow::compute::Filter(&function_context,
                                   left_blocks[i]->get_records()->column(i_i),
                                   *left_filter,
                                   out_data);
        if (!status.ok()) {
          // filter failed
          evaluate_status(status, __FUNCTION__, __LINE__);
        }
        out_record_data[i_i].push_back(std::make_shared<arrow::compute::Datum>(out_data));
      }
      for (int i_i = 0; i_i < right_blocks[i]->get_records()->num_columns(); i_i++) {
        if (right_blocks[i]->get_records()->schema()->field(i_i)->name() != column_name_) {
          auto *out_data = new arrow::compute::Datum();
          status =
              arrow::compute::Filter(&function_context,
                                     left_blocks[i]->get_records()->column(i_i),
                                     *right_filter,
                                     out_data);
          if (!status.ok()) {
            // filter failed
            evaluate_status(status, __FUNCTION__, __LINE__);
          }
          out_record_data[left_blocks[i]->get_records()->num_columns()
              + i_i].push_back(std::make_shared<arrow::compute::Datum>(out_data));
        }
      }
    }
    // convert vector of vectors to vector of arrays
    auto out_arrays = std::vector<std::shared_ptr<arrow::Array>>();
    for (auto &j : out_record_data) {
      std::shared_ptr<arrow::Array> out_array = nullptr;
      arrow::Int64Builder array_builder = arrow::Int64Builder();
      for (const auto &jj : j) {
        auto a_value = std::static_pointer_cast<arrow::Int64Scalar>(jj->scalar());
        status = array_builder.Append(a_value->value);
        if (!status.ok()) {
          // ArrayBuilder append failed
          evaluate_status(status, __FUNCTION__, __LINE__);
        }
      }
      status = array_builder.Finish(&out_array);
      if (!status.ok()) {
        // ArrayBuilder finish failed
        evaluate_status(status, __FUNCTION__, __LINE__);
      }
      out_arrays.push_back(out_array);
    }
    // create RecordBatch and Block from result
    auto out_batch = arrow::RecordBatch::Make(out_schema, out_arrays[0]->length(), out_arrays);
    std::shared_ptr<Block> out_block = std::make_shared<Block>(Block(rand(), out_batch, BLOCK_SIZE));
    out.push_back(out_block);
  }
  return out;
}

} // namespace operators
} // namespace hustle
