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
#define debugmsg(arg) {std::cout << __FILE_NAME__ << ":" << __LINE__ << " " << arg << std::endl;}


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
  // TODO: We assume each chunk has the same number of data.
  //   It is not always true. The correct algorithm is to
  //      linearly traverse the chunkarray.
  //   Change this algorithm to a generate (and yield the
  //      result each time we calls it).
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
  : BaseAggregate(query_id, options),
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
      Finish(internal);
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

  // Initialize the group_by_cols_
  for (std::size_t group_index = 0; group_index < group_by_refs_.size();
       group_index++) {
    ctx->spawnTask(CreateLambdaTask(
      [this, group_index](Task* internal){
        group_by_tables_[group_index].get_column_by_name(
          internal,group_by_refs_[group_index].col_name, group_by_cols_[group_index]);
      }
      ));
  }

  // Initialize the Tuple Map
  tuple_map = new phmap::parallel_flat_hash_map<hash_t, std::tuple<int, int>>();

}

void HashAggregate::ComputeAggregates(Task *ctx) {
  // TODO: Only support one column aggregation for now.
  ctx->spawnTask(CreateTaskChain(
    CreateLambdaTask([this](Task* internal){
      // Initialize the agg_col_ variable.
      auto table = aggregate_refs_[0].col_ref.table;
      auto col_name = aggregate_refs_[0].col_ref.col_name;
      agg_lazy_table_ = prev_result_->get_table(table);
      agg_lazy_table_.get_column_by_name(internal, col_name, agg_col_);
    }),
    CreateLambdaTask([this](Task* internal){
      // Partition the chunks.

      // Initialize the aggregate_col_data_?
      auto agg_col = agg_col_.chunked_array();
      auto num_chunks = agg_col->num_chunks();
      // TODO: Maybe this is unnecessary
      aggregate_col_data_.resize(num_chunks);

      for (std::size_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
        auto val = agg_col->chunk(chunk_index)->data()->GetValues<int64_t>(1, 0);
        aggregate_col_data_[chunk_index] = val;
      }

      // Distribute tasks to perform the first phase of hash aggregate.
      std::size_t totalThreads = std::thread::hardware_concurrency();
      // TODO: Initialize the strategy at the Aggregate init time.
      // TODO: Make strategy control the hash function.
      strategy = HashAggregateStrategy(totalThreads, num_chunks);
      auto num_tasks = strategy.suggestedNumTasks();
      InitializeLocalHashTables();

    }),
    CreateLambdaTask([this](Task* internal){
      // First Phase Aggregate
      auto num_tasks = strategy.suggestedNumTasks();
      std::size_t totalThreads = std::thread::hardware_concurrency();

      auto agg_col = agg_col_.chunked_array();
      auto num_chunks = agg_col->num_chunks();

      for (size_t tid = 0; tid < num_tasks; ++tid) {
        int st, ed;
        std::tie (st, ed) = strategy.getChunkID(
          tid, totalThreads, num_chunks);
        if (st >= ed){ continue; }

        // First Phase Aggregation
        internal->spawnTask(CreateLambdaTask(
          [this, tid, st, ed](Task * ctx){
//            printf("tid=%zu, st=%d, ed=%d\n", tid, st, ed);
            FirstPhaseAggregateChunks(tid, st, ed);
          }
        ));
      }
    }),
    CreateLambdaTask([this](Task* internal){
      // Second Phase Aggregate
      // TODO: Make strategy controls whether we use the phmap or
      //  the single hash map strategy.
      // 1. Initialize the global map
      // 2. For each local map, we add that to the global map.
      // 3. Convert to an arrow array.

      // TODO: Refactor this strategy and use the parallel update strategy.
      //  Haven't figure out if the phmap has the compare-then-swap mechamism.
      // TODO: Optimize this strategy - first take the largest map,
      //  then for other maps just update the other map.
      //  Be careful when there is only one map.
      SecondPhaseAggregate(internal);

    })

    ));
}

void HashAggregate::SecondPhaseAggregate(Task* internal){
  auto kernel = aggregate_refs_[0].kernel;
  switch (kernel) {
    case SUM:
    case COUNT:{
      auto big_map = new HashMap();
      global_map = big_map;
      for (auto map: value_maps){
        for(auto item: *map){
          auto key = item.first;
          auto value = item.second;
          auto it = big_map->find(key);
          if (it == big_map->end()){
            big_map->insert(std::make_pair(key, value));
          }else{
            it->second = value + it->second;
          }
        }
      }
      break;
    }

    case MEAN: {
      auto big_value_map = new HashMap();
      auto big_count_map = new HashMap();
      auto big_final_map = new MeanHashMap();
      global_map = big_final_map;

      // First aggregate all the result to
      // big_value_map and big_count_map
      for (int i = 0; i < value_maps.size(); ++i) {
        auto cmap = count_maps.at(i);
        auto vmap = value_maps.at(i);
        for (auto ct: *cmap) {
          auto key = ct.first;
          auto count = ct.second;
          auto vt = vmap->find(key);
          if (vt == vmap->end()) {
            std::cerr << "Failed to find the key in value table: "
                      << key << std::endl;
            throw std::exception();
          }
          auto value = vt->second;

          // Update the global value map
          auto it = big_value_map->find(key);
          if (it == big_value_map->end()) {
            big_value_map->insert(std::make_pair(key, value));
          } else {
            it->second = it->second + value;
          }
          // Update the global count map
          auto jt = big_count_map->find(key);
          if (jt == big_count_map->end()) {
            big_count_map->insert(std::make_pair(key, count));
          } else {
            it->second = it->second + count;
          }
        }
      }

      // Then update the global result map
      for(auto ct: *big_count_map){
        auto key = ct.first;
        auto count = ct.second;
        auto vt = big_value_map->find(key);
        if (vt == big_value_map->end()){
          std::cerr << "Failed to find the key in value table: "
                    << key << std::endl;
          throw std::exception();
        }
        auto value = vt->second;

        auto result = (double) value / (double) count;
        big_final_map->insert({key, result});
      }

      break;
    }
  }

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
  HashMap * count_map = nullptr;
  HashMap * value_map = nullptr;
  if (kernel == SUM || kernel == COUNT){
    value_map = value_maps.at(tid);
  }else if (kernel == MEAN){
    count_map = count_maps.at(tid);
    value_map = value_maps.at(tid);
  }

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
          auto d = std::static_pointer_cast<arrow::StringArray>(group_by_chunk);
          auto c = d->GetString(item_index);
          auto b = std::hash<std::string>{}(c);
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
          auto d = std::static_pointer_cast<arrow::Int64Array>(group_by_chunk);
          int64_t b = d->Value(item_index);
          auto e = std::hash<std::int64_t>{}(b);
          next_key = e;
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
//    debugmsg(agg_item_key);

    // 2
    // TODO: Without a unique operator, we don't need to calculate
    //  the String type hash code.
    switch (agg_type_id) {
      case arrow::Type::STRING: {
        // SUM, COUNT, MEAN
        assert (kernel != MEAN);
        assert (kernel != SUM);
        agg_item_value = 1;
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY :{
        // TODO: Not very sure what the FIXED_SIZE_BINARY can do...
        assert (kernel != MEAN);
        assert (kernel != SUM);
        agg_item_value = 1;
        break;
      }
      case arrow::Type::INT64 :{
        // SUM, COUNT, MEAN
        switch (kernel) {
          case SUM:
          case MEAN:{
            auto d = std::static_pointer_cast<arrow::Int64Array>(agg_chunk);
            int64_t b = d->Value(item_index);
            agg_item_value = b;
            break;
          }
          case COUNT: {
            agg_item_value = 1;
            break;
          }
        }

        break;
      }
      default: {
        std::cerr << "HashAggregate does not support aggregate of type " +
                     agg_type->ToString()
                  << std::endl;
      }
    }


    // 3
    // TODO: Optimize the hash key access pattern.
    switch (kernel) {
      case SUM:{
        auto it = value_map->find(agg_item_key);
        if (it == value_map->end()){
          value_map->insert(std::make_pair(agg_item_key, agg_item_value));
        }else{
          it->second = agg_item_value + it->second;
        }
        break;
      }

      case COUNT:{
        auto it = value_map->find(agg_item_key);
        if (it == value_map->end()){
          value_map->insert(std::make_pair(agg_item_key, 1));
        }else{
          it->second = 1 + it->second;
        }
        break;
      }

      case MEAN:{
        auto it = value_map->find(agg_item_key);
        auto jt = count_map->find(agg_item_key);
        if (it == value_map->end()){
          value_map->insert(std::make_pair(agg_item_key, agg_item_value));
          count_map->insert(std::make_pair(agg_item_key, 1));
        }else{
          it->second = agg_item_value + it->second;
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
//    tuple_map->insert_or_assign(agg_item_key, item);
//    tuple_map->insert_or_assign(0, item);
    // TODO: Optimize using the folloiwng code...
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

void HashAggregate::Finish(Task* ctx) {

  // Finish the aggregate and output the column.
  //  Procedure:
  //    1. Construct the resulting group-by columns
  //    2. Construct the resulting aggregate table
  //    3. Construct the final table for output.

  // 1. Construct the resulting group-by columns
  std::vector<std::shared_ptr<arrow::Array>> group_by_arrays;
  group_by_arrays.reserve(group_by_cols_.size());

  for(int i = 0; i < group_by_cols_.size(); i++){

    auto group_by = group_by_cols_.at(i);

    auto group_by_type = group_by.type();
    auto group_by_type_id = group_by_type->id();

    // The following step assume the iteration of tuple map
    //    is not going to mutate.
    // TODO: This is a very very tedious construction of the group_by builder.
    //  Try to optimize it by using the good features within the arrow.
    switch (group_by_type_id) {
      case arrow::Type::STRING :{
        arrow::StringBuilder builder;
          for(auto const it: *tuple_map){
            int chunk_id, item_index;
            auto hash_key = it.first;
            std::tie(chunk_id, item_index) = it.second;
            auto raw_chunk = group_by.chunks().at(chunk_id);
            auto chunk = std::static_pointer_cast<arrow::StringArray>(raw_chunk);
            // TODO: Optimize this to make it std::move()?
            std::string value = chunk->GetString(item_index);
            // TODO: Try to use the UnsafeAppend here,
            //  but encounter an error.
//            builder.UnsafeAppend(value);
            builder.Append(value);
          }
        std::shared_ptr<arrow::Array> array;
        arrow::Status st = builder.Finish(&array);
        if (!st.ok()) {
          std::cerr << "Building the array for "
                    << i << "th groupby column failed." << std::endl;
          exit(1);
        }
        group_by_arrays.push_back(array);
        break;
      }
      case arrow::Type::INT64 :{
        arrow::Int64Builder builder;
        for(auto const it: *tuple_map){
          int chunk_id, item_index;
          auto hash_key = it.first;
          std::tie(chunk_id, item_index) = it.second;
          auto raw_chunk = group_by.chunks().at(chunk_id);
          auto chunk = std::static_pointer_cast<arrow::Int64Array>(raw_chunk);
          int64_t value = chunk->Value(item_index);
//          builder.UnsafeAppend(value);
          builder.Append(value);
        }
        std::shared_ptr<arrow::Array> array;
        arrow::Status st = builder.Finish(&array);
        if (!st.ok()) {
          std::cerr << "Building the array for "
                    << i << "th groupby column failed." << std::endl;
          exit(1);
        }
        group_by_arrays.push_back(array);
        break;
      }
      default:{
        std::cerr << "Not supported aggregate group by column type: "
                  << group_by_type_id
                  << std::endl;
        exit(1);
      }
    }
  }

  // 2. Construct the resulting aggregate table
  auto kernel = aggregate_refs_[0].kernel;
  std::shared_ptr<arrow::Array> agg_array;
  switch (kernel) {
    case SUM:
    case COUNT:{
      auto map = static_cast<HashMap *>(global_map);
      arrow::Int64Builder builder;
      for(auto const it: *tuple_map){
        auto hash_key = it.first;
        auto value = map->find(hash_key)->second;
        builder.Append(value);
        // UnsafeAppend
      }
      arrow::Status st = builder.Finish(&agg_array);
      if (!st.ok()) {
        std::cerr << "Building the array for aggregate column failed." << std::endl;
        exit(1);
      }
      break;
    }
    case MEAN: {
      auto map = static_cast<MeanHashMap *>(global_map);
      arrow::DoubleBuilder builder;
      for(auto const it: *tuple_map){
        int chunk_id, item_index;
        auto hash_key = it.first;
        auto value = map->find(hash_key)->second;
        builder.Append(value);
      }
      arrow::Status st = builder.Finish(&agg_array);
      if (!st.ok()) {
        std::cerr << "Building the array for aggregate column failed." << std::endl;
        exit(1);
      }
      break;
    }
    default: {
      std::cerr << "Not supported kernel" << std::endl;
      exit(1);
    }
  }


  // 3. Sort the result
  std::vector<arrow::Datum> groups;
  arrow::Datum aggregates;

  groups.reserve(group_by_arrays.size());
  for(auto & group_by_array : group_by_arrays){
    auto pt = group_by_array->data();
    groups.emplace_back(pt);
  }
  aggregates.value = agg_array->data();

  SortResult(groups, aggregates);

  // 4. Construct the final table for output.
  std::vector<std::shared_ptr<arrow::ArrayData>> output_table_data;
  output_table_data.reserve(group_by_arrays.size() + 1);
  for (auto& group_values : groups) {
    output_table_data.push_back(group_values.make_array()->data());
  }
  output_table_data.push_back(aggregates.make_array()->data());

  output_table_->insert_records(output_table_data);
  output_result_->append(output_table_);

}

void HashAggregate::SortResult(std::vector<arrow::Datum>& groups,
                           arrow::Datum& aggregates) {
  // The columns in the GROUP BY and ORDER BY clause may not directly correspond
  // to the same column, e.g we may have
  // GROUP BY R.a, R.b
  // ORDER BY R.b, R.a
  // order_by_group[i] is the index at which the ith ORDER BY column appears in
  // the GROUP BY clause. In this example, order_to_group = {1, 0}
  std::vector<int> order_to_group;

  for (auto& order_by_ref : order_by_refs_) {
    for (std::size_t j = 0; j < group_by_refs_.size(); ++j) {
      if (order_by_ref.table == group_by_refs_[j].table) {
        order_to_group.push_back(j);
      }
    }
  }

  arrow::Datum sorted_indices;
  arrow::Status status;

  // Assume that indices are correct and that boundschecking is unecessary.
  // CHANGE TO TRUE IF YOU ARE DEBUGGING
  arrow::compute::TakeOptions take_options(true);

  // If we are sorting after computing all aggregates, we evaluate the ORDER BY
  // clause in reverse order.
  for (int i = order_by_refs_.size() - 1; i >= 0; i--) {
    auto order_ref = order_by_refs_[i];
    // A nullptr indicates that we are sorting by the aggregate column
    // TODO(nicholas): better way to indicate we want to sort the aggregate?
    if (order_ref.table == nullptr) {
      status = arrow::compute::SortToIndices(*aggregates.make_array())
        .Value(&sorted_indices);
      evaluate_status(status, __FUNCTION__, __LINE__);
    } else {
      auto group = groups[order_to_group[i]];
      status = arrow::compute::SortToIndices(*group.make_array())
        .Value(&sorted_indices);
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
    status = arrow::compute::Take(aggregates, sorted_indices, take_options)
      .Value(&aggregates);
    evaluate_status(status, __FUNCTION__, __LINE__);

    for (auto& group : groups) {
      status = arrow::compute::Take(group, sorted_indices, take_options)
        .Value(&group);
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
  }
}

}