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

    switch(values.kind()) {
        case arrow::compute::Datum::NONE:
            break;
        case arrow::compute::Datum::ARRAY: {
            status = arrow::compute::Filter(&function_context,
                                            values,
                                            filter.make_array(),
                                            filter_options,
                                            out);
            break;
        }
        case arrow::compute::Datum::CHUNKED_ARRAY: {
            status = arrow::compute::Filter(&function_context,
                                            values,
                                            filter.chunked_array(),
                                            filter_options,
                                            out);
            break;
        }
        default: {
            std::cerr << "Value kind not supported" << std::endl;
        }
    }

    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void apply_indices(
    const arrow::compute::Datum& values,
    const arrow::compute::Datum& indices,
    arrow::compute::Datum* out) {

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
            std::cerr << "Value kind not supported" << std::endl;
    }


    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void sort_to_indices(const arrow::compute::Datum& values, arrow::compute::Datum* out) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::shared_ptr<arrow::Array> temp;
    status = arrow::compute::SortToIndices(&function_context, *values.make_array().get(), &temp);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

    out->value = temp->data();
}

void sort_datum(const arrow::compute::Datum& values, arrow::compute::Datum* out) {

    arrow::compute::Datum sorted_indices;

    sort_to_indices(values, &sorted_indices);
    apply_indices(values, sorted_indices, out);
}


void compare(
    const arrow::compute::Datum& left,
    const arrow::compute::Datum& right,
    arrow::compute::CompareOperator compare_operator,
    arrow::compute::Datum* out) {

    arrow::Status status;
    arrow::compute::CompareOptions compare_options(compare_operator);
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    status = arrow::compute::Compare(
        &function_context, left, right, compare_options, out);
    evaluate_status(status, __FUNCTION__, __LINE__);
}

void unique(const arrow::compute::Datum& values, arrow::compute::Datum* out) {

    arrow::Status status;
    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    std::shared_ptr<arrow::Array> unique_values;

    std::shared_ptr<arrow::Array> temp_array;
    // Fetch the unique values in group_by_col
    status = arrow::compute::Unique(&function_context, values, &temp_array);
    evaluate_status(status, __FUNCTION__, __LINE__);

    out->value = temp_array->data();
}

}