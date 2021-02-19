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

#ifndef HUSTLE_BLOCK_METADATA_SMA
#define HUSTLE_BLOCK_METADATA_SMA

#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/scalar.h>

#include "storage/block_metadata.h"

namespace hustle::storage {

class Sma : public BlockMetadata {
 public:
  /**
   * Construct a Small Materialized Aggregate (SMA) metadata from a
   * given ArrayData. This stores Min and Max values.
   *
   * @param data
   */
  Sma(const std::shared_ptr<arrow::Array>& array);

  /// implementation of GetStatus
  inline arrow::Status GetStatus() override { return status_; }

  /// implementation of Search
  bool Search(const arrow::Datum& val_ptr,
              arrow::compute::CompareOperator compare_operator) override;

 private:
  /// status from calculating min and max
  arrow::Status status_;

  /// min value
  arrow::Datum min_;

  /// max value
  arrow::Datum max_;
};
}  // namespace hustle::storage
#endif  // HUSTLE_BLOCK_METADATA_SMA
