#ifndef HUSTLE_ARROW_COMPUTE_WRAPPERS_H
#define HUSTLE_ARROW_COMPUTE_WRAPPERS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "scheduler/task.h"

namespace hustle {

class Context {
 public:
  Context();

  arrow::Datum out_;

  void apply_indices(Task *ctx, arrow::Datum values, arrow::Datum indices,
                     arrow::Datum index_chunks, arrow::Datum &out);
  void match(Task *ctx, const arrow::Datum &values, const arrow::Datum &keys,
             arrow::Datum &out);
  void apply_filter(Task *ctx, const arrow::Datum &values,
                    const arrow::Datum &filter, arrow::Datum &out);

 private:
  int slice_length_;
  void clear_data();

  arrow::ArrayVector array_vec_;

  void apply_indices_internal_str(
      const std::shared_ptr<arrow::ChunkedArray> &chunked_values,
      const std::shared_ptr<arrow::Array> &indices_array,
      const std::shared_ptr<arrow::Array> &offsets, int slice_i);

  template <typename T>
  void apply_indices_internal(
      const std::shared_ptr<arrow::ChunkedArray> &chunked_values,
      const T **values_data_vec,
      const std::shared_ptr<arrow::Array> &indices_array,
      const std::shared_ptr<arrow::Array> &offsets, int slice_i);

  template <typename T>
  void apply_indices_internal2(
      const std::shared_ptr<arrow::ChunkedArray> &chunked_values,
      const T **values_data_vec,
      const std::shared_ptr<arrow::Array> &indices_array,
      const std::shared_ptr<arrow::Array> &index_chunks,
      const std::shared_ptr<arrow::Array> &offsets, int slice_i);

  void apply_filter_internal(Task *ctx, const arrow::Datum &values,
                             const arrow::Datum &filter,
                             arrow::ArrayVector &out);

  arrow::Datum apply_filter_block(const std::shared_ptr<arrow::Array> &values,
                                  const std::shared_ptr<arrow::Array> &filter,
                                  arrow::ArrayVector &out);
};

}  // namespace hustle
#endif  // HUSTLE_ARROW_COMPUTE_WRAPPERS_H
