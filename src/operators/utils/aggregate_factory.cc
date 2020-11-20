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

#include "aggregate_factory.h"


namespace hustle::operators {

BaseAggregate * get_agg_op(
  const std::size_t query_id,
  const AggregateType aggregate_type,
  const std::shared_ptr<OperatorResult> &prev_result,
  const std::shared_ptr<OperatorResult> &output_result,
  const std::vector<AggregateReference> &aggregate_units,
  const std::vector<ColumnReference> &group_by_refs,
  const std::vector<ColumnReference> &order_by_refs,
  const std::shared_ptr<OperatorOptions> &options) {

  if (aggregate_type == AggregateType::ARROW_AGGREGATE) {
    return new Aggregate(
      query_id, prev_result, output_result,
      aggregate_units, group_by_refs, order_by_refs, options);

  } else if (aggregate_type == AggregateType::HASH_AGGREGATE) {
    return new HashAggregate(
      query_id, prev_result, output_result,
      aggregate_units, group_by_refs, order_by_refs, options);
  } else {
    throw std::invalid_argument("Unexpected AggregateType");
  }
}

}
