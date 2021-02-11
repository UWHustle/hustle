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

#ifndef HUSTLE_AGGREGATE_OPTIONS_H
#define HUSTLE_AGGREGATE_OPTIONS_H

#include "operators/aggregate/aggregate_const.h"
#include "operators/utils/operator_options.h"

namespace hustle::operators {

/**
 * The AggregateOptions define the scope of configurable parameters that
 * user can choose when aggregation happens.
 * TODO: Expand the options to configurate the aggregation.
 */
class AggregateOptions : public OperatorOptions {
 public:
  explicit AggregateOptions() : aggregateType(AggregateType::ARROW_AGGREGATE){};

  double get_aggregate_type() const { return aggregateType; }
  /**
   * Select the aggregate algorithm (arrow.compute or our own hash aggregate)
   */
  AggregateOptions& set_aggregate_type(AggregateType type) {
    aggregateType = type;
    return *this;
  }

 private:
  AggregateType aggregateType;
};

}  // namespace hustle::operators

#endif  // HUSTLE_AGGREGATE_OPTIONS_H
