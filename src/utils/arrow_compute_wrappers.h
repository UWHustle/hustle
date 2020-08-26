#ifndef HUSTLE_ARROW_COMPUTE_WRAPPERS_H
#define HUSTLE_ARROW_COMPUTE_WRAPPERS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <scheduler/Task.hpp>


namespace hustle {

class Context {
public:

    Context();

    arrow::Datum out_;

    void apply_indices(Task *ctx, arrow::Datum values, arrow::Datum indices, arrow::Datum index_chunks,
                  arrow::Datum &out);
    void match(Task *ctx, const arrow::Datum &values, const arrow::Datum &keys, arrow::Datum &out);

private:

    int slice_length_;
    void clear_data();


    arrow::ArrayVector array_vec_;

    void apply_indices_internal_str(const std::shared_ptr<arrow::ChunkedArray> &chunked_values,
                                    const std::shared_ptr<arrow::Array> &indices_array,
                                    const std::shared_ptr<arrow::Array> &offsets, int slice_i);

    template<typename T>
    void apply_indices_internal(const std::shared_ptr<arrow::ChunkedArray> &chunked_values, const T**values_data_vec,
                                const std::shared_ptr<arrow::Array> &indices_array,
                                const std::shared_ptr<arrow::Array> &offsets, int slice_i);


    template<typename T>
    void apply_indices_internal2(const std::shared_ptr<arrow::ChunkedArray> &chunked_values, const T **values_data_vec,
                                 const std::shared_ptr<arrow::Array> &indices_array,
                                 const std::shared_ptr<arrow::Array> &index_chunks,
                                 const std::shared_ptr<arrow::Array> &offsets, int slice_i);
};

}
#endif //HUSTLE_ARROW_COMPUTE_WRAPPERS_H
