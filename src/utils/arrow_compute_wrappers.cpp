#include <assert.h>
#include "arrow_compute_wrappers.h"
#include "../table/util.h"

namespace hustle {

void Context::apply_filter_internal(
    Task* ctx,
    const arrow::Datum& values,
    const arrow::Datum& filter,
    arrow::ArrayVector& out) {

    ctx->spawnLambdaTask([this, values, filter, &out](Task* internal) {
        auto chunked_values = values.chunked_array();
        auto chunked_filter = filter.chunked_array();

        out.resize(chunked_values->num_chunks());

    if (chunked_filter->num_chunks() != chunked_values->num_chunks()) {
        int num_chunks = chunked_values->num_chunks();

        std::vector<int64_t> slice_offsets(num_chunks);
        std::vector<int64_t> slice_length(num_chunks);
        slice_offsets[0] = 0;

        std::vector<int64_t> chunk_row_offsets(num_chunks+1);
        chunk_row_offsets[0] = 0;
        for (int i = 1; i < num_chunks; i++) {
            chunk_row_offsets[i] =
                chunk_row_offsets[i - 1] + chunked_values->chunk(i - 1)->length();
        }
        chunk_row_offsets[num_chunks] = chunk_row_offsets[num_chunks-1] +
                                        chunked_values->chunk(num_chunks-1)->length();

        arrow::ArrayVector vec;

        std::shared_ptr<arrow::Array> array_filter;
        auto status = arrow::Concatenate(chunked_filter->chunks(), arrow::default_memory_pool(), &array_filter);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);


        for (int i=0; i<num_chunks; i++) {

            auto x = (chunk_row_offsets[i]);
            int y =  chunk_row_offsets[i+1]-chunk_row_offsets[i];
//            if (int i=405) std::cout << array_filter->ToString() << std::endl;
            auto slice = array_filter->Slice(chunk_row_offsets[i], chunk_row_offsets[i+1]-chunk_row_offsets[i]);
            vec.push_back(slice);
        }

        chunked_filter = std::make_shared<arrow::ChunkedArray>(vec);

//        std::shared_ptr<arrow::Array> array_filter;
//        auto status = arrow::Concatenate(chunked_values->chunks(), arrow::default_memory_pool(), &array_filter);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//        chunked_values = std::make_shared<arrow::ChunkedArray>(array_filter);
//
//        status = arrow::Concatenate(chunked_filter->chunks(), arrow::default_memory_pool(), &array_filter);
//        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//        chunked_filter = std::make_shared<arrow::ChunkedArray>(array_filter);
//        std::cout << "was here" << std::endl;

//        for (int i=0; i<chunked_values->num_chunks(); i++) {
//            out[i] = chunked_values->chunk(i);
//        }
//        return;
    }


        for (int i=0; i<chunked_filter->num_chunks(); i++) {
//            internal->spawnLambdaTask([this, i, &out, chunked_filter, chunked_values] {

                arrow::Status status;

                arrow::Datum block_filter;
//                std::cout << "apply filter" << std::endl;
//                std::cout << chunked_values->length() << " " << chunked_filter->length() << std::endl;
//                std::cout << chunked_values->num_chunks() << " " << chunked_filter->num_chunks()<< std::endl;
                status = arrow::compute::Filter(chunked_values->chunk(i),
                                                chunked_filter->chunk(i)).Value(&block_filter);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                out[i] = block_filter.make_array();
//            });
        }
    });
}

void Context::apply_filter(
    Task* ctx,
    const arrow::Datum& values,
    const arrow::Datum& filter,
    arrow::Datum& out) {

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask([this, values, filter, &out](Task* internal) {
            clear_data();

            arrow::Status status;

            switch(values.kind()) {
                case arrow::Datum::NONE:
                    break;
                case arrow::Datum::ARRAY: {
                    status = arrow::compute::Filter(values,
                                                    filter.make_array()).Value(&out);
                    break;
                }
                case arrow::Datum::CHUNKED_ARRAY: {

            apply_filter_internal(internal, values, filter, array_vec_);
                    break;
                }
                default: {
                    std::cerr << "Value kind not supported" << std::endl;
                }
            }

            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        }),
        CreateLambdaTask([this, values, filter, &out](Task* internal) {
            if (values.kind() == arrow::Datum::CHUNKED_ARRAY) {
                auto chunked_values = values.chunked_array();
                auto chunked_filter = filter.chunked_array();
                out.value = std::make_shared<arrow::ChunkedArray>(array_vec_);

            }
        })
    ));

}

void apply_filter(
    const arrow::Datum& values,
    const arrow::Datum& filter,
    arrow::Datum* out) {

    arrow::Status status;
    switch(values.kind()) {
        case arrow::Datum::NONE:
            break;
        case arrow::Datum::ARRAY: {
            status = arrow::compute::Filter(values,filter.make_array()).Value(out);
            break;
        }
        case arrow::Datum::CHUNKED_ARRAY: {
            status = arrow::compute::Filter(values,filter.chunked_array()).Value(out);
            break;
        }
        default: {
            std::cerr << "Value kind not supported" << std::endl;
        }
    }
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

void apply_indices(
    const arrow::Datum& values,
    const arrow::Datum& indices,
    arrow::Datum* out) {

    arrow::Status status;
    arrow::compute::TakeOptions take_options;

    switch(values.kind()) {
        case arrow::Datum::NONE:
            break;
        case arrow::Datum::ARRAY:
            status = arrow::compute::Take(values.make_array(), indices.make_array()).Value(out);
            break;
        case arrow::Datum::CHUNKED_ARRAY: {
            std::shared_ptr<arrow::ChunkedArray> temp_chunked_array;
            status = arrow::compute::Take(
                *values.chunked_array(), *indices.make_array()).Value(&temp_chunked_array);
            out->value = temp_chunked_array;
            break;
        }
        default:
            std::cerr << "Value kind not supported" << std::endl;
    }
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
}

Context::Context() {

}

void Context::apply_indices(
    Task* ctx,
    const arrow::Datum values,
    const arrow::Datum indices,
    bool has_sorted_indices,
    arrow::Datum& out) {
//
//    clear_data();
//    ctx->spawnTask(CreateTaskChain(
//        CreateLambdaTask([this, values, indices, has_sorted_indices, &out](Task* internal)  {
//
//            arrow::Status status;
//
//            std::shared_ptr<arrow::ChunkedArray> temp_chunked_array;
//
//            arrow::Datum sorted_indices;
//            if (has_sorted_indices) {
//                sorted_indices = indices;
//            }
////            else {
//                // TODO(nicholas): We do not need to sort the indices; we can get simply
//                //  sort TO indices and then iterate over those indice instead!
//                sort_datum(indices, &sorted_indices);
////            }
//
//            std::shared_ptr<arrow::ChunkedArray> chunked_values;
//            switch(values.kind()){
//
//                case arrow::Datum::ARRAY:
//                    chunked_values = std::make_shared<arrow::ChunkedArray>(values.make_array());
//                    break;
//                case arrow::Datum::CHUNKED_ARRAY:
//                    chunked_values = values.chunked_array();
//                    break;
//
//                default: {
//                    std::cerr << "Unexpected values kind" << std::endl;
//                }
//            }
//            int s1 = sorted_indices.chunks().size();
//            int s2 = values.chunks().size();
//
//            std::shared_ptr<arrow::Array> indices_array;
//
//            if (sorted_indices.kind() == arrow::Datum::CHUNKED_ARRAY) {
//                assert(sorted_indices.chunked_array()->num_chunks() == 1);
//                indices_array = sorted_indices.chunked_array()->chunk(0);
//            }
//            else {
//                indices_array = sorted_indices.make_array();
//            }
//            auto indices_data = indices_array->data()->GetMutableValues<int64_t>(1, 0);
//
//            int num_chunks = chunked_values->num_chunks();
//
//            std::vector<int64_t> slice_offsets(num_chunks);
//            std::vector<int64_t> slice_length(num_chunks);
//            slice_offsets[0] = 0;
//
//
//            std::vector<int64_t> chunk_row_offsets(num_chunks+1);
//            chunk_row_offsets[0] = 0;
//            for (int i = 1; i < num_chunks; i++) {
//                chunk_row_offsets[i] =
//                    chunk_row_offsets[i - 1] + chunked_values->chunk(i - 1)->length();
//            }
//            chunk_row_offsets[num_chunks] = chunk_row_offsets[num_chunks-1] +
//                                            chunked_values->chunk(num_chunks-1)->length();
//            bool end = false;
//
//
//            for (int i = 0; i < chunked_values->num_chunks(); i++) {
//                if (end) break;
//
//                int length_of_slice = 0;
//                int max_index = chunk_row_offsets[i+1]-1;
//
//                for (int j = slice_offsets[i]; j < sorted_indices.length(); j++) {
//                    if (indices_data[j] > max_index) {
//                        break;
//                    }
//                    else {
//                        length_of_slice++;
//                        indices_data[j] -= chunk_row_offsets[i];
//                    }
//                }
//                slice_offsets[i+1] = slice_offsets[i] + length_of_slice;
//                slice_length[i] = length_of_slice;
//            }
//
//            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//
//            array_vec_.resize(chunked_values->num_chunks());
//
//            for (int i=0; i<chunked_values->num_chunks(); i++) {
//                if (slice_length[i] == 0) {
//                    array_vec_[i] = arrow::MakeArrayOfNull(chunked_values->type(), 0, arrow::default_memory_pool()).ValueOrDie();
//                    continue;
//                }
//                internal->spawnLambdaTask([this, indices_array, slice_length, slice_offsets, chunked_values, i]{
//                    auto sliced_indices = indices_array->Slice(slice_offsets[i], slice_length[i]);
//
//                    auto chunk = chunked_values->chunk(i);
//                    std::shared_ptr<arrow::Array> temp_array;
//
//                    arrow::Status status;
//                    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
//                    arrow::compute::TakeOptions take_options;
//
//                    status = arrow::compute::Take(
//                        &function_context,
//                        *chunk,
//                        *sliced_indices,
//                        take_options, &temp_array);
//
//                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
//                    array_vec_[i] = temp_array;
//                });
//            }
//        }),
//        CreateLambdaTask([this, values, indices, has_sorted_indices, &out](Task* internal)  {
//
//            arrow::Status status;
//            std::shared_ptr<arrow::Array> arr;
//            out.value = std::make_shared<arrow::ChunkedArray>(array_vec_);
//            out_ = std::make_shared<arrow::ChunkedArray>(array_vec_);
//
//        })
//    ));





}

void Context::clear_data() {
    array_vec_.clear();

}


void sort_to_indices(const arrow::Datum& values, arrow::Datum* out) {

    arrow::Status status;

    std::shared_ptr<arrow::Array> temp;
    if (values.kind() == arrow::Datum::CHUNKED_ARRAY) {
        std::shared_ptr<arrow::Array> combined_chunks;
        status = arrow::Concatenate(values.chunked_array()->chunks(),
                                    arrow::default_memory_pool(),
                                    &combined_chunks);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

        status = arrow::compute::SortToIndices(*combined_chunks).Value(&temp);
    } else {
        status = arrow::compute::SortToIndices(*values.make_array().get()).Value(&temp);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }
    out->value = temp->data();
}

void sort_datum(const arrow::Datum& values, arrow::Datum* out) {

    arrow::Datum sorted_indices;

    sort_to_indices(values, &sorted_indices);
    apply_indices(values, sorted_indices, out);
}


void compare(
    const arrow::Datum& left,
    const arrow::Datum& right,
    arrow::compute::CompareOperator compare_operator,
    arrow::Datum* out) {

    arrow::Status status;
    arrow::compute::CompareOptions compare_options(compare_operator);
    status = arrow::compute::Compare(left, right, compare_options).Value(out);
    evaluate_status(status, __FUNCTION__, __LINE__);
}

void unique(const arrow::Datum& values, arrow::Datum* out) {

    arrow::Status status;
    std::shared_ptr<arrow::Array> unique_values;

    std::shared_ptr<arrow::Array> temp_array;
    // Fetch the unique values in group_by_col
    status = arrow::compute::Unique(values).Value(&temp_array);
    evaluate_status(status, __FUNCTION__, __LINE__);


    out->value = temp_array->data();
}

}