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

#include "hash_aggregate.h"

// TODO: Merge this reference with the one in aggregate.cc.
//  Maybe put this in aggregate.h?
#define AGGREGATE_OUTPUT_TABLE "aggregate"

namespace hustle::operators {


HashAggregate::HashAggregate(const std::size_t query_id,
                             std::shared_ptr<OperatorResult> prev_result,
                             std::shared_ptr<OperatorResult> output_result,
                             std::vector<AggregateReference> aggregate_refs,
                             std::vector<ColumnReference> group_by_refs,
                             std::vector<ColumnReference> order_by_refs)
  : HashAggregate(
        query_id, prev_result, output_result, aggregate_refs,
        group_by_refs, order_by_refs,
        std::make_shared<OperatorOptions>()) {}

HashAggregate::HashAggregate(const std::size_t query_id,
                             std::shared_ptr<OperatorResult> prev_result,
                             std::shared_ptr<OperatorResult> output_result,
                             std::vector<AggregateReference> aggregate_refs,
                             std::vector<ColumnReference> group_by_refs,
                             std::vector<ColumnReference> order_by_refs,
                             std::shared_ptr<OperatorOptions> options)
  : Operator(query_id, options),
    prev_result_(prev_result),
    output_result_(output_result),
    aggregate_refs_(aggregate_refs),
    group_by_refs_(group_by_refs),
    order_by_refs_(order_by_refs) {}

void HashAggregate::execute(Task *ctx) {
  ctx->spawnTask(CreateTaskChain(
    CreateLambdaTask([this](Task* internal){
      Initialize(internal);
    }),
    CreateLambdaTask([this](Task* internal){
      ComputeAggregates(internal);
    }),
    CreateLambdaTask([this](Task* internal){
      Finish();
    })
    ));
}

void HashAggregate::Initialize(Task *ctx) {

  // Fetch the fields associated with each groupby column.
  std::vector<std::shared_ptr<arrow::Field>> group_by_fields;
  group_by_fields.reserve(group_by_refs_.size());
  for (auto& group_by : group_by_refs_) {
    group_by_fields.push_back(
      group_by.table->get_schema()->GetFieldByName(group_by.col_name));
  }

  // Initialize a StructBuilder containing one builder for each group
  // by column.
  group_type_ = arrow::struct_(group_by_fields);
  group_builder_ = std::make_shared<arrow::StructBuilder>(
    group_type_, arrow::default_memory_pool(), CreateGroupBuilderVector());

  // Initialize aggregate builder
  aggregate_builder_ = CreateAggregateBuilder(aggregate_refs_[0].kernel);

  // Initialize output table and its schema. group_type_ must be initialized
  // beforehand.
  std::shared_ptr<arrow::Schema> out_schema =
    OutputSchema(aggregate_refs_[0].kernel, aggregate_refs_[0].agg_name);
  output_table_ =
    std::make_shared<Table>(AGGREGATE_OUTPUT_TABLE, out_schema, BLOCK_SIZE);

  group_by_cols_.resize(group_by_refs_.size());
  for (auto& group_by : group_by_refs_) {
    group_by_tables_.push_back(prev_result_->get_table(group_by.table));
  }



}

void HashAggregate::ComputeAggregates(Task *internal) {

}

void HashAggregate::Finish() {

}

std::vector<std::shared_ptr<arrow::ArrayBuilder>>
HashAggregate::CreateGroupBuilderVector() {
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
  for (auto& field : group_type_->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::STRING: {
        group_builders.push_back(std::make_shared<arrow::StringBuilder>());
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        group_builders.push_back(
          std::make_shared<arrow::FixedSizeBinaryBuilder>(field->type()));
        break;
      }
      case arrow::Type::INT64: {
        group_builders.push_back(std::make_shared<arrow::Int64Builder>());
        break;
      }
      default: {
        std::cerr << "Aggregate does not support group bys of type " +
                     field->type()->ToString()
                  << std::endl;
      }
    }
  }
  return group_builders;
}


}