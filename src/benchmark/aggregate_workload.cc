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


#include <operators/utils/aggregate_factory.h>
#include "aggregate_workload.h"

namespace hustle::operators {

AggregateWorkload::AggregateWorkload(int cardinality, int numGroupby) {

  this->cardinality = cardinality;
  this->num_group_by = numGroupby;

  auto aggregate_parallel_factor = std::thread::hardware_concurrency();
  this->num_threads_ = aggregate_parallel_factor;

  this->aggregate_options = std::make_shared<OperatorOptions>();
  aggregate_options->set_parallel_factor(aggregate_parallel_factor);

  this->scheduler = std::make_shared<Scheduler>(num_threads_, true);
};


void AggregateWorkload::prepareData() {

  // Build schema of the input table
  std::vector<std::shared_ptr<arrow::Field>> fields;

  for (int i = 0; i < 1 + num_group_by; ++i) {
    auto colname = "Col" + std::to_string(i);
    fields.push_back(arrow::field(colname, arrow::int64()));

    std::shared_ptr<arrow::Int64Array> col;

    arrow::Int64Builder builder;
    builder.Reserve(cardinality);
    for (int j = 0; j < cardinality; ++j) {
      builder.UnsafeAppend(j);
    }
    builder.Finish(&col);

    inputData.push_back(col->data());
  }

  schema = arrow::schema(fields);

  inputTable = std::make_shared<hustle::storage::DBTable>(
    "table", schema, BLOCK_SIZE);

  inputTable->insert_records(inputData);
}

std::string
AggregateWorkload::eventName(AggregateType t, const std::string & name) {
  if (t == AggregateType::ARROW_AGGREGATE) {
    return name + "_arrow";
  } else if (t == AggregateType::HASH_AGGREGATE) {
    return name + "_hash";
  }
  throw std::runtime_error("Unkown aggregate type");
}

void AggregateWorkload::q1(AggregateType agg_type) {

  auto input_ = std::make_shared<OperatorResult>();
  auto out_result = std::make_shared<OperatorResult>();
  input_->append(inputTable);

  AggregateReference agg_ref = {
    AggregateKernel::MEAN, "agg_col", inputTable, "Col0"};
  std::vector<ColumnReference> group_by_refs;
  std::vector<ColumnReference> output_refs;

  output_refs.push_back({nullptr, "agg_col"});

  for (int i = 0; i < num_group_by; ++i) {
    auto name = "Col" + std::to_string(i + 1);
    group_by_refs.push_back({inputTable, name});
    output_refs.push_back({nullptr, name});
  }

  BaseAggregate *agg_op = get_agg_op(
    0, agg_type, input_, out_result,
    {agg_ref}, group_by_refs, group_by_refs,
    aggregate_options);

  ExecutionPlan plan(0);
  plan.addOperator(agg_op);

  auto container = simple_profiler.getContainer();
  auto name = "Q1_" + std::to_string(cardinality) + "*" +
                      std::to_string(num_group_by);
  auto eventName = this->eventName(agg_type, name);
  scheduler->addTask(&plan);
  container->startEvent(eventName);
  scheduler->start();
  scheduler->join();
  container->endEvent(eventName);

  if (print_) {
    auto out_table = out_result->materialize(output_refs);
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);
  }

  simple_profiler.clear();

};


}