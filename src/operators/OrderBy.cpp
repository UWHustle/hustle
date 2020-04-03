//
// Created by Nicholas Corrado on 3/26/20.
//

#include "OrderBy.h"
#include <arrow/compute/api.h>

#include <utility>
#include "../table/util.h"

namespace hustle {
namespace operators {

OrderBy::OrderBy(const std::shared_ptr<Table>& table,
                                    std::shared_ptr<arrow::Field> order_by_field) {

    order_by_field_ = std::move(order_by_field);

}

std::shared_ptr<Table> OrderBy::order_by(std::shared_ptr<Table> table) {

    arrow::Status status;

    auto out_table = std::make_shared<Table>("table", table->get_schema(),
                                             BLOCK_SIZE);

    arrow::compute::FunctionContext function_context
    (arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> sorted_indices;
    arrow::ArrayVector sorted_indices_vector;

    // TODO(nicholas): Is there a way to combine all chunks in a ChunkedArray
    //  without making an Arrow table first?
    auto order_by_col = table->get_column_by_name(order_by_field_->name());

    // TODO(nicholas): SortToIndices cannot take a ChunkedArray as a
    //  parameter, so if we want to sort our column, we have to combine
    //  all of its chunks. This is very costly, as it requires memory
    //  copying.
//    status = arrow::compute::SortToIndices(
//            &function_context,
//            *order_by_col,
//            &sorted_indices);
//    evaluate_status(status, __FUNCTION__, __LINE__);
    sorted_indices_vector.push_back(sorted_indices);


    for (int j=0; j<out_table->get_num_cols(); j++) {
//        arrow::compute::Take()
    }

    return std::shared_ptr<Table>();
}

}
}