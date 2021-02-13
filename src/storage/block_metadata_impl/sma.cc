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

#include "storage/block_metadata_impl/sma.h"

namespace hustle::storage {

Sma::Sma(const std::shared_ptr<arrow::Array>& array) {
  arrow::compute::MinMaxOptions options;
  options.null_handling = arrow::compute::MinMaxOptions::SKIP;  // skip nulls
  arrow::Result<arrow::Datum> min_max_output =
      arrow::compute::MinMax(array, options);
  ok = min_max_output.ok();
  status = min_max_output.status();
  if (ok) {
    const auto& min_max_scalar = static_cast<const arrow::StructScalar&>(
        *min_max_output.ValueOrDie().scalar());
    min = min_max_scalar.value[0];
    max = min_max_scalar.value[1];
  }
}

bool Sma::Search(const arrow::Datum& val_ptr,
                 arrow::compute::CompareOperator compare_operator) {
  arrow::Result<arrow::Datum> result;
  switch (compare_operator) {
    case arrow::compute::CompareOperator::GREATER:
    case arrow::compute::CompareOperator::GREATER_EQUAL:
      result = arrow::compute::Compare(
          max, val_ptr, arrow::compute::CompareOptions(compare_operator));
      break;
    case arrow::compute::CompareOperator::LESS:
    case arrow::compute::CompareOperator::LESS_EQUAL:
      result = arrow::compute::Compare(
          min, val_ptr, arrow::compute::CompareOptions(compare_operator));
      break;
    case arrow::compute::CompareOperator::EQUAL:
    case arrow::compute::CompareOperator::NOT_EQUAL:
    default:
      return true;
  }
  if (!result.ok()) {
    return true;
  }
  return static_cast<arrow::BooleanScalar&>(*result.ValueOrDie().scalar())
      .data();
}

}  // namespace hustle::storage