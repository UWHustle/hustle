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

#ifndef HUSTLE_AGGREGATE_FACTORY_H
#define HUSTLE_AGGREGATE_FACTORY_H

#include "operators/aggregate/base_aggregate.h"
#include "operators/aggregate/aggregate_const.h"
#include "operators/aggregate/aggregate.h"
#include "operators/aggregate/hash_aggregate.h"

namespace hustle::operators {

/**
 * Return the aggregate operator, with respect to the aggregate_type.
 *
 * @param query_id Query ID.
 * @param aggregate_type AggregateType.
 * @param prev_result Input for aggregation.
 * @param output_result Output from the aggregate operator.
 * @param aggregate_units
 * @param group_by_refs
 * @param order_by_refs
 * @param options
 * @return
 */
BaseAggregate * get_agg_op(
  std::size_t query_id,
  AggregateType aggregate_type,
  const OperatorResult::OpResultPtr &prev_result,
  const OperatorResult::OpResultPtr &output_result,
  const std::vector<AggregateReference> &aggregate_units,
  const std::vector<ColumnReference> &group_by_refs,
  const std::vector<ColumnReference> &order_by_refs,
  const std::shared_ptr<OperatorOptions> &options);

} // namespace hustle::operators

#endif //HUSTLE_AGGREGATE_FACTORY_H
