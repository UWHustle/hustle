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
Aggregate::Aggregate(AggregateKernels aggregate_kernel,
                     std::string column_name) {
    aggregate_kernel_ = aggregate_kernel;
    column_name_ = std::move(column_name);
}

std::vector<std::shared_ptr<Block>> Aggregate::runOperator(
        std::vector<std::vector<std::shared_ptr<Block>>> block_groups) {
    // operator only uses first block group, ignore others
    auto blocks = block_groups[0];
    //
    arrow::Status status;
    auto out = std::vector<std::shared_ptr<Block>>();

    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
        array_vector.push_back(blocks[i]->get_records()->GetColumnByName(
                column_name_));
    }

    auto chunked_array = std::make_shared<arrow::ChunkedArray>(array_vector);
    arrow::compute::Datum full_column(chunked_array);
        // select on each block separately
        // setup
        std::vector<std::shared_ptr<arrow::ArrayData>> out_data;
        arrow::compute::FunctionContext function_context(
                arrow::default_memory_pool());
        std::vector<arrow::compute::Datum*> out_aggregates;
        auto *out_aggregate = new arrow::compute::Datum();
        // computation

            switch (aggregate_kernel_) {
                case SUM: {
                    status = arrow::compute::Sum(
                            &function_context,
                            full_column,
                            out_aggregate
                    );
                    evaluate_status(status, __FUNCTION__, __LINE__);
//                    out_aggregates.push_back(out_aggregate);
                }
                    break;
                case COUNT: {
                    // TODO(martin): count options
                    // TODO(nicholas): Currently, Count cannot accept
                    //  ChunkedArray Datums. Support for ChunkedArray Datums
                    //  was recently added (late January) for Sum and Mean,
                    //  and it seems reasonable to assume that it will be
                    //  implemented for Count soon too. Once support is
                    //  added, we can remove the line below.
                    throw std::runtime_error("Count aggregate not supported.");
                    auto count_options = arrow::compute::CountOptions(
                            arrow::compute::CountOptions::COUNT_ALL);
                    status = arrow::compute::Count(
                            &function_context,
                            count_options,
                            full_column,
                            out_aggregate
                    );
                    evaluate_status(status, __FUNCTION__, __LINE__);
                }
                    break;
                    // NOTE: Mean outputs a DOUBLE
                case MEAN: {
                    status = arrow::compute::Mean(
                            &function_context,
                            full_column,
                            out_aggregate
                    );
                    evaluate_status(status, __FUNCTION__, __LINE__);
                }
                    break;
            }
            evaluate_status(status, __FUNCTION__, __LINE__);

            std::shared_ptr<arrow::Array> out_array;
            status = arrow::MakeArrayFromScalar(
                    arrow::default_memory_pool(),
                    *out_aggregate->scalar(), 1, &out_array);
            evaluate_status(status, __FUNCTION__, __LINE__);
            out_data.push_back(out_array->data());

        // create RecordBatch and Block from result
//    auto out_batch = arrow::RecordBatch::Make(arrow::schema({blocks[i]->get_records()->schema()->GetFieldByName(column_name_)}), out_data[0]->length, out_data);
        auto out_batch = arrow::RecordBatch::Make(arrow::schema(
                {arrow::field("mean", arrow::float64())}),
                                                  out_data[0]->length,
                                                  out_data);
        std::shared_ptr<Block> out_block = std::make_shared<Block>(
                Block(rand
                              (), out_batch, BLOCK_SIZE));
        out.push_back(out_block);
        out_data.clear();
    return out;
}

} // namespace operators
} // namespace hustle
