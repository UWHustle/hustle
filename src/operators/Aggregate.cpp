#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>

namespace hustle {
namespace operators {

/**
 *
 * @param column_name
 */
Aggregate::Aggregate(AggregateKernels aggregate_kernel, std::string column_name) {
  aggregate_kernel_ = aggregate_kernel;
  column_name_ = std::move(column_name);
}

std::vector<std::shared_ptr<Block>> Aggregate::runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
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
    auto *out_aggregate = new arrow::compute::Datum();
    // computation
    auto *out_col = new arrow::compute::Datum();
    for (int j = 0; j < blocks[i]->get_records()->schema()->num_fields(); j++) {
      switch(aggregate_kernel_){
        case SUM: {
          status = arrow::compute::Sum(
              &function_context,
              blocks[i]->get_records()->GetColumnByName(column_name_),
              out_aggregate
              );
          if (!status.ok()) {
            // SUM aggregate kernel failed
            evaluate_status(status, __FUNCTION__, __LINE__);
          }
        }
        break;
        case COUNT: {
          //TODO(martin) count options
          auto count_options = arrow::compute::CountOptions(arrow::compute::CountOptions::COUNT_ALL);
          status = arrow::compute::Count(
              &function_context,
              count_options,
              blocks[i]->get_records()->GetColumnByName(column_name_),
              out_aggregate
          );
          if (!status.ok()) {
            // COUNT aggregate kernel failed
            evaluate_status(status, __FUNCTION__, __LINE__);
          }
        }
        break;
        case MEAN: {
          status = arrow::compute::Mean(
              &function_context,
              blocks[i]->get_records()->GetColumnByName(column_name_),
              out_aggregate
          );
          if (!status.ok()) {
            // MEAN aggregate kernel failed
            evaluate_status(status, __FUNCTION__, __LINE__);
          }
        }
        break;
      }
      if (!status.ok()) {
        // aggregate failed
        evaluate_status(status, __FUNCTION__, __LINE__);
      }
      std::shared_ptr<arrow::Array> out_array;
      status = arrow::MakeArrayFromScalar(arrow::default_memory_pool(), *out_aggregate->scalar(), 1, &out_array);
      evaluate_status(status, __FUNCTION__, __LINE__);
      out_data.push_back(out_array->data());
    }
    // create RecordBatch and Block from result
    auto out_batch = arrow::RecordBatch::Make(arrow::schema({blocks[i]->get_records()->schema()->GetFieldByName(column_name_)}), out_data[0]->length, out_data);
    std::shared_ptr<Block> out_block = std::make_shared<Block>(Block(rand(), out_batch, out_data[0]->length));
    out.push_back(out_block);
    out_data.clear();
  }
  return out;
}

} // namespace operators
} // namespace hustle
