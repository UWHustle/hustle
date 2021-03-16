// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_ARROW_COMPUTE_WRAPPERS_H
#define HUSTLE_ARROW_COMPUTE_WRAPPERS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "scheduler/task.h"
#include "scheduler/threading/synchronization_lock.h"

namespace hustle {

class Context {
 public:
  Context();

  arrow::Datum out_;

  void apply_indices(Task *ctx, arrow::Datum values, arrow::Datum indices,
                     arrow::Datum index_chunks, arrow::Datum &out);
  void match(Task *ctx, const arrow::Datum &values, const arrow::Datum &keys);
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
