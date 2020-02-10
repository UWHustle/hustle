#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>

#define BLOCK_SIZE 1024

namespace hustle {
namespace operators {

Select::Select(arrow::compute::CompareOperator compare_operator, std::string column_name, arrow::compute::Datum column_value) {
  compare_operator_ = compare_operator;
  column_name_ = std::move(column_name);
  column_value_ = std::move(column_value);
}

std::vector<std::shared_ptr<Block>> Select::runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
  // operator only uses first block group, ignore others
  auto blocks = block_groups[0];
  //
  arrow::Status status;
  auto out = std::vector<std::shared_ptr<Block>>();
  for (int i = 0; i < blocks.size(); i++) {
    // select on each block separately
    // setup
    std::vector<std::shared_ptr<arrow::ArrayData>> out_data;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(compare_operator_);
    auto* filter = new arrow::compute::Datum();
    // computation
    status = arrow::compute::Compare(
        &function_context,
        blocks[i]->get_records()->GetColumnByName(column_name_),
        column_value_,
        compare_options,
        filter
        );
    if (!status.ok()) {
      // compare failed, could not generate filter
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
    auto* out_col = new arrow::compute::Datum();
    for (int j = 0; j < blocks[i]->get_records()->schema()->num_fields(); j++) {
      status = arrow::compute::Filter(&function_context, blocks[i]->get_records()->column(j), *filter, out_col);
      if(!status.ok()) {
        // filter failed
        evaluate_status(status, __FUNCTION__, __LINE__);
      }
      out_data.push_back(out_col->array());
    }
    // create RecordBatch and Block from result
    // copy schema
    auto out_batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(arrow::Schema(*blocks[i]->get_records()->schema())), out_data[0]->length, out_data);
    std::shared_ptr<Block> out_block = std::make_shared<Block>(Block(rand(), out_batch, BLOCK_SIZE));
    out.push_back(out_block);
    out_data.clear();
  }
  return out;
}

} // namespace operators
} // namespace hustle
