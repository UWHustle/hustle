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

#include <storage/util.h>
#include "hash_aggregate.h"

// TODO: Merge this reference with the one in aggregate.cc.
//  Maybe put this in aggregate.h?
#define AGGREGATE_OUTPUT_TABLE "aggregate"

namespace hustle::operators {

HashAggregateStrategy::HashAggregateStrategy()
  : partitions(0), chunks(0) {}

HashAggregateStrategy::HashAggregateStrategy(int partitions,
                                             int chunks)
  : partitions(partitions), chunks(chunks) {}

int HashAggregateStrategy::suggestedNumTasks() const {
    if (chunks < partitions){
      return chunks;
    }
    return partitions;
}

std::tuple<int, int> HashAggregateStrategy::getChunkID(
  int tid, int totalThreads, int totalNumChunks){

  assert(tid >= 0);
  assert(totalThreads > 0);
  assert(totalNumChunks >= 0);

  if (tid >= totalNumChunks){
    return std::make_tuple(-1, -1);
  }

  //  auto M = ceil(totalNumChunks / totalThreads);
  int M = (totalNumChunks + totalThreads - 1) / totalThreads;
  //  auto m = floor(totalNumChunks / totalThreads);
  int m = totalNumChunks / totalThreads;
  int FR = totalNumChunks % totalThreads;

  int st = 0;
  int ed = 0;
  if (tid < FR){
    st = M * tid;
    ed = M * (tid + 1);
  }else{
    st = M * FR + m * (tid - FR);
    ed = M * FR + m * (tid + 1- FR);
  }
  // st = max(st, 0);
  st = st < 0 ? 0 : st;
  // ed = min(ed, totalNumChunks);
  ed = ed > totalNumChunks ? totalNumChunks : ed;

  return std::make_tuple(st, ed);
};



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
    })
    ,
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
    auto val = group_by.table->get_schema()->GetFieldByName(group_by.col_name);
    group_by_fields.push_back(val);
    debugmsg(val->ToString(true));

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

    }),
    CreateLambdaTask([this](Task * internal){
      // Second Phase Aggregate
      // TODO: Make strategy controls whether we use the phmap or
      //  the single hash map strategy.
      // 1. Initialize the global map
      // 2. For each local map, we add that to the global map.
      // 3. Convert to an arrow array.



    })

    ));
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

std::shared_ptr<arrow::ArrayBuilder> HashAggregate::CreateAggregateBuilder(
  AggregateKernel kernel) {
  std::shared_ptr<arrow::ArrayBuilder> aggregate_builder;
  switch (kernel) {
    case SUM: {
      aggregate_builder = std::make_shared<arrow::Int64Builder>();
      break;
    }
    case MEAN: {
      aggregate_builder = std::make_shared<arrow::DoubleBuilder>();
      break;
    }
    case COUNT: {
      aggregate_builder = std::make_shared<arrow::Int64Builder>();
      break;
    }
  }
  return aggregate_builder;
}

std::shared_ptr<arrow::Schema> HashAggregate::OutputSchema(
  AggregateKernel kernel, const std::string& agg_col_name) {
  arrow::Status status;
  arrow::SchemaBuilder schema_builder;

  if (group_type_ != nullptr) {
    status = schema_builder.AddFields(group_type_->fields());
    evaluate_status(status, __FUNCTION__, __LINE__);
  }
  switch (kernel) {
    case SUM: {
      status =
        schema_builder.AddField(arrow::field(agg_col_name, arrow::int64()));
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }
    case MEAN: {
      status =
        schema_builder.AddField(arrow::field(agg_col_name, arrow::float64()));
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }
    case COUNT: {
      status =
        schema_builder.AddField(arrow::field(agg_col_name, arrow::int64()));
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }
  }

  auto result = schema_builder.Finish();
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  return result.ValueOrDie();
}

// TODO: Refactor HashCombine function as a part of the strategy.
// TODO: Verify if this hash function works.
HashAggregate::hash_t HashAggregate::HashCombine(hash_t seed, hash_t val){
  seed ^= val + 0x9e3779b9 + (seed<<6) + (seed>>2);
  return seed;
}

void HashAggregate::FirstPhaseAggregateChunks(size_t tid, int st, int ed) {
  for(int i = st; i < ed; i++){
    FirstPhaseAggregateChunk_(tid, i);
  }
}

void HashAggregate::FirstPhaseAggregateChunk_(size_t tid, int chunk_index) {
  auto agg_chunk = agg_col_.chunked_array()->chunk(chunk_index);
  auto chunk_size = agg_chunk->length();

  auto agg_type = agg_col_.type();
  auto agg_type_id = agg_type->id();

  auto kernel = aggregate_refs_[0].kernel;

  // Retrieve the proper maps for aggregate
  HashMap * count_map = count_maps.at(tid);
  HashMap * value_map = value_maps.at(tid);

  for (int item_index = 0; item_index < chunk_size; ++item_index) {
    // TODO: Redefine item value as hash_t
    // 1. Build agg_item_key
    // 2. Build agg_item_value
    // 3. Insert the value into the hash table
    // 4. If the key did not present in 3 originally,
    //      insert the tuple into tuple_map.
    hash_t agg_item_key = 0;
    long long agg_item_value = 0;

    // 1
    hash_t key = 0;
    for (const auto& group_by: group_by_cols_){
      auto group_by_chunk = group_by.chunked_array()->chunk(chunk_index);

      auto group_by_type = group_by.type();
      auto group_by_type_id = group_by_type->id();
      hash_t next_key = 0;
      // TODO: Factor this out as template functions.
      switch (group_by_type_id) {
        case arrow::Type::STRING : {
          // TODO: Guard - cannot be sum / mean, but could be count.
          auto a = group_by_chunk->data()->GetValues<std::string>(item_index)->c_str();
          auto b = std::hash<std::string>{}(a);
          next_key = b;
          break;
        }
        case arrow::Type::FIXED_SIZE_BINARY : {
          // TODO: Not sure what is the FIXED_SIZE_BINARY type.
          std::cerr << "HashAggregate does not support group bys of type " +
                    group_by_type->ToString()
                    << std::endl;
          break;
        }
        case arrow::Type::INT64 : {
          auto a = group_by_chunk->data()->GetValues<std::int64_t>(item_index);
          next_key = *a;
          break;
        }
        default: {
          std::cerr << "HashAggregate does not support group bys of type " +
                    group_by_type->ToString()
                    << std::endl;
        }
      }
      key = HashCombine(key, next_key);
    }
    agg_item_key = key;
    debugmsg(agg_item_key);

    // 2
    // TODO: Without a unique operator, we don't need to calculate
    //  the String type hash code.
    switch (agg_type_id) {
      case arrow::Type::STRING : {
        // TODO: Guard - cannot be sum / mean, but could be count.
        auto a = agg_chunk->data()->GetValues<std::string>(item_index)->c_str();
        auto b = std::hash<std::string>{}(a);
        agg_item_value = b;
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY : {
        // TODO: Not sure what is the FIXED_SIZE_BINARY type.
        std::cerr << "HashAggregate does not support aggregate of type " +
                     agg_type->ToString()
                  << std::endl;
        break;
      }
      case arrow::Type::INT64 : {
        auto a = agg_chunk->data()->GetValues<std::int64_t>(item_index);
        agg_item_value = *a;
        break;
      }
      default: {
        std::cerr << "HashAggregate does not support aggregate of type " +
                     agg_type->ToString()
                  << std::endl;
      }
    }
    debugmsg(agg_item_value);


    // 3
    // TODO: Optimize the hash key access pattern.
    switch (kernel) {
      case SUM:{
        auto it = value_map->find(agg_item_key);
        if (it != value_map->end()){
          it->second = agg_item_value;
        }else{
          it->second = agg_item_value + it->second;
        }
        break;
      }

      case COUNT:{
        auto it = value_map->find(agg_item_key);
        if (it != value_map->end()){
          it->second = 1;
        }else{
          it->second = 1 + it->second;
        }
        break;
      }

      case MEAN:{
        auto it = value_map->find(agg_item_key);
        auto jt = count_map->find(agg_item_key);
        if (it != value_map->end()){
          it->second = agg_item_value;
        }else{
          it->second = agg_item_value + it->second;
        }
        if (jt != count_map->end()){
          jt->second = 1;
        }else{
          jt->second = 1 + jt->second;
        }
        break;
      }

      default:{
        std::cerr << "Not supported aggregate kernel." << std::endl;
        break;
      }
    }

    // 4
    auto it = tuple_map->find(agg_item_key);
    if (it == tuple_map->end()){
      // Only insert the tuple when its not shown.
      // TODO: insert_or_assign() does not prevent collision when two threads
      //  tries to enter the critical section. A better method, which does not
      //  show up here, seems to be
      //        insert_or_abort().
      auto item = std::tuple(chunk_index, item_index);
      tuple_map->insert_or_assign(agg_item_key, item);
    }
  }

}

void HashAggregate::InitializeLocalHashTables() {
  auto num_tasks = strategy.suggestedNumTasks();
  auto kernel = aggregate_refs_[0].kernel;
  switch (kernel) {
    case SUM:
    case COUNT: {
      for (size_t tid = 0; tid < num_tasks; ++tid) {
        auto map = new HashMap();
        value_maps.push_back(map);
      }
      break;
    }

    case MEAN: {
      // Need 2 maps to hold the float values accountable.
      for (size_t tid = 0; tid < num_tasks; ++tid) {
        auto value_map = new HashMap();
        auto count_map = new HashMap();
        value_maps.push_back(value_map);
        count_maps.push_back(count_map);
      }
      break;
    }
  }
}



}