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


#ifndef HUSTLE_AGGREGATE_WORKLOAD_H
#define HUSTLE_AGGREGATE_WORKLOAD_H

#include <operators/aggregate_const.h>
#include <execution/execution_plan.h>
#include <operators/aggregate.h>
#include <operators/hash_aggregate.h>
#include <scheduler/scheduler.h>

namespace hustle::operators {

class AggregateWorkload {
public:

  explicit AggregateWorkload(int cardinality, int numGroupby);
  void setPrint(bool shouldPrint){print_ = shouldPrint;}

  void prepareData();

  void q1(AggregateType agg_type);


private:
  bool print_ = false;
  int cardinality;
  int num_group_by;
  int num_threads_;
  std::shared_ptr<Scheduler> scheduler;

  std::shared_ptr<OperatorOptions> aggregate_options;

  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<hustle::storage::DBTable> inputTable;
  std::vector<std::shared_ptr<arrow::ArrayData>> inputData;

  std::string eventName(AggregateType t, const std::string &name);
};


} // namespace hustle::operators

#endif //HUSTLE_AGGREGATE_WORKLOAD_H
