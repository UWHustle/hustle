#include <assert.h>
#include "arrow_compute_wrappers.h"
#include "../table/util.h"

namespace hustle {


void apply_filter(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& filter,
    arrow::compute::Datum* out) {

    assert(filter.kind() == arrow::compute::Datum::CHUNKED_ARRAY);

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
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& indices,
    arrow::compute::Datum* out) {

    assert(indices.kind() == arrow::compute::Datum::ARRAY);

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;

    switch(values.kind()) {
        case arrow::compute::Datum::NONE:
            break;
        case arrow::compute::Datum::ARRAY:
            status = arrow::compute::Take(
                &function_context,
                values.make_array(),
                indices.make_array(),
                take_options, out);
            break;
        case arrow::compute::Datum::CHUNKED_ARRAY: {
            std::shared_ptr<arrow::ChunkedArray> temp_chunked_array;
            status = arrow::compute::Take(
                &function_context,
                *values.chunked_array(),
                *indices.make_array(),
                take_options, &temp_chunked_array);
            out->value = temp_chunked_array;
            break;
        }
        default:
            std::cerr << "Value array kind not supported" << std::endl;
    }


    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void sort_to_indices(const std::shared_ptr<arrow::Array>& values, std::shared_ptr<arrow::Array>* out) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    status = arrow::compute::SortToIndices(&function_context, *values, out);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void sort_to_indices(const arrow::compute::Datum& values, arrow::compute::Datum* out) {

    assert(values.kind() == arrow::compute::Datum::ARRAY);

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::shared_ptr<arrow::Array> temp;
    status = arrow::compute::SortToIndices(&function_context, *values.make_array().get(), &temp);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    out->value = temp->data();

}




}