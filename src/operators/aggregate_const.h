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

#ifndef HUSTLE_AGGREGATE_CONST_H
#define HUSTLE_AGGREGATE_CONST_H

#include "operators/operator.h"
#include "base_aggregate.h"

namespace hustle::operators {


// Types of aggregates we can perform. COUNT is currently not supported.
enum AggregateKernel {
  SUM, COUNT, MEAN
};

// Types of aggregate algorithm we can use.
enum AggregateType {
  HASH_AGGREGATE, ARROW_AGGREGATE
};

//std::string aggTypeName(AggregateType t) {
//  switch (t) {
//    case HASH_AGGREGATE:
//      return "hash_aggregate";
//    case ARROW_AGGREGATE:
//      return "arrow_aggregate";
//    default:
//      return "unkown_aggregate";
//  }
//}

/**
 * A reference structure containing all the information needed to perform an
 * aggregate over a column.
 *
 * @param kernel The type of aggregate we want to compute
 * @param agg_name Name of the new aggregate column
 * @param col_ref A reference to the column over which we want to compute the
 * aggregate
 */
struct AggregateReference {
  AggregateKernel kernel;
  std::string agg_name;
  ColumnReference col_ref;
};

}

#endif //HUSTLE_AGGREGATE_CONST_H
