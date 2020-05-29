#include <assert.h>
#include "arrow_compute_wrappers.h"
#include "../table/util.h"

namespace hustle {


void apply_filter(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& filter,
    arrow::compute::Datum* out) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::FilterOptions filter_options;

    status = arrow::compute::Filter(&function_context,
                                    values,
                                    filter.chunked_array(),
                                    filter_options,
                                    out);

    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void apply_indices(
    const std::shared_ptr<arrow::ChunkedArray>& values,
    const arrow::compute::Datum& indices,
    std::shared_ptr<arrow::ChunkedArray>* out) {

    assert(indices.kind() == arrow::compute::Datum::ARRAY);

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;

    status = arrow::compute::Take(
        &function_context,
        *values,
        *indices.make_array(),
        take_options, out);

    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}




}