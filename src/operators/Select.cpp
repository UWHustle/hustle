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

// TODO(nicholas): returned blocks are generally NOT full. In the future, we
//   will want to fill the blocks are much as possible.
std::shared_ptr<Block> Select::runOperatorOnBlock(std::shared_ptr<Block>
        input_block) {

    arrow::Status status;

    // TODO(nicholas): Should the function context be created in the inner or
    //  outer loop? I think we only need it in the inner loop if selects are
    //  being executed in parallel
    std::vector<std::shared_ptr<arrow::ArrayData>> out_data;
    auto memory_pool = arrow::default_memory_pool();
    arrow::compute::FunctionContext function_context(memory_pool);
    arrow::compute::CompareOptions compare_options(compare_operator_);
    auto* filter = new arrow::compute::Datum();

    status = arrow::compute::Compare(
            &function_context,
            input_block->get_column_by_name(column_name_),
            column_value_,
            compare_options,
            filter
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    // Check if selectivity is 0 i.e. no tuples match the select predicate
    if (filter->make_array()->null_count() == filter->length() ) {
        return nullptr;
    }

    auto* out_col = new arrow::compute::Datum();
    for (int j = 0; j < input_block->get_records()->num_columns(); j++) {
        status = arrow::compute::Filter(&function_context,
                input_block->get_column(j),
                                        *filter, out_col);

        evaluate_status(status, __FUNCTION__, __LINE__);
        out_data.push_back(out_col->array());
    }

    // TODO(nicholas): Block constructor should accept vector of ArrayData,
    //  not vector of RecordBatches
    auto out_batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>
            (arrow::Schema(*input_block->get_records()->schema())),
            out_data[0]->length, out_data);

    return  std::make_shared<Block>(Block(rand(), out_batch, BLOCK_SIZE));
}

std::shared_ptr<Table> Select::runOperator
(std::vector<std::shared_ptr<Table>> tables) {
  // operator only uses first block group, ignore others
    auto table = tables[0];
    //
    arrow::Status status;

    auto memory_pool = arrow::default_memory_pool();
    arrow::compute::FunctionContext function_context(memory_pool);
    arrow::compute::CompareOptions compare_options(compare_operator_);
    auto* filter_datum = new arrow::compute::Datum();
    arrow::ArrayVector filter_array_vector;

    // TODO(nicholas): Arrow currenlty doesn't supported Compare with
    //  ChunkedArray, so we just run COmpare on each block individually and
    //  combine the results into one ChunkedArray ourselves.
    for (int i=0; i<table->get_num_blocks(); i++) {
        status = arrow::compute::Compare(
                &function_context,
                table->get_block(i)->get_column_by_name(column_name_),
                column_value_,
                compare_options,
                filter_datum
        );
        evaluate_status(status, __FUNCTION__, __LINE__);
        filter_array_vector.push_back(filter_datum->make_array());
    }
    auto filter = std::make_shared<arrow::ChunkedArray>(filter_array_vector);

    // Check if selectivity is 0 i.e. no tuples match the select predicate
    // TODO(nicholas): this is never true!
    if (filter->null_count() == filter->length() ) {
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;
    std::shared_ptr<arrow::ChunkedArray> out_col;

    for (int j = 0; j < table->get_schema()->num_fields(); j++) {
        status = arrow::compute::Filter(&function_context,
                                        *table->get_column(j),
                                        *filter, &out_col);

        evaluate_status(status, __FUNCTION__, __LINE__);
        out_table_data.push_back(out_col);
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;
    int out_block_counter = 0;
    auto out_table = std::make_shared<Table>("out", table->get_schema(),
                                             BLOCK_SIZE);

    for (int chunk_i=0; chunk_i<out_table_data[0]->num_chunks();
         chunk_i++) {
        int c = 0;
        for (auto &col : out_table_data) {
            out_block_data.push_back(col->chunk(chunk_i)->data());
        }

        out_table->insert_records(out_block_data);
        out_block_data.clear();
    }

    return out_table;
}

} // namespace operators
} // namespace hustle
