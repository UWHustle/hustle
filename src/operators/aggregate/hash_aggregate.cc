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

#include "operators/aggregate/hash_aggregate.h"

#include <storage/utils/util.h>

#include "type/type_helper.h"

// TODO: Merge this reference with the one in aggregate.cc.
//  Maybe put this in aggregate.h?
#define AGGREGATE_OUTPUT_TABLE "aggregate"
#define debugmsg(arg) \
  { std::cout << __FILE_NAME__ << ":" << __LINE__ << " " << arg << std::endl; }

namespace hustle::operators {

HashAggregate::HashAggregate(const std::size_t query_id,
                             OperatorResult::OpResultPtr prev_result,
                             OperatorResult::OpResultPtr output_result,
                             std::vector<AggregateReference> aggregate_refs,
                             std::vector<ColumnReference> group_by_refs,
                             std::vector<ColumnReference> order_by_refs)
    : HashAggregate(query_id, prev_result, output_result, aggregate_refs,
                    group_by_refs, order_by_refs,
                    std::make_shared<OperatorOptions>()) {}

HashAggregate::HashAggregate(const std::size_t query_id,
                             OperatorResult::OpResultPtr prev_result,
                             OperatorResult::OpResultPtr output_result,
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
      CreateLambdaTask([this](Task *internal) { Initialize(internal); }),
      CreateLambdaTask([this](Task *internal) { ComputeAggregates(internal); }),
      CreateLambdaTask([this](Task *internal) { Finish(internal); })));
}

void HashAggregate::Initialize(Task *ctx) {
  // Fetch the fields associated with each groupby column.
  std::vector<std::shared_ptr<arrow::Field>> group_by_fields;
  group_by_fields.reserve(group_by_refs_.size());
  for (auto &group_by : group_by_refs_) {
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
      std::make_shared<DBTable>(AGGREGATE_OUTPUT_TABLE, out_schema, BLOCK_SIZE);
  group_by_cols_.resize(group_by_refs_.size());
  for (auto &group_by : group_by_refs_) {
    group_by_tables_.push_back(prev_result_->get_table(group_by.table));
  }

  // Initialize the group_by_cols_
  for (std::size_t group_index = 0; group_index < group_by_refs_.size();
       group_index++) {
    ctx->spawnTask(CreateLambdaTask([this, group_index](Task *internal) {
      group_by_tables_[group_index].MaterializeColumn(
          internal, group_by_refs_[group_index].col_name,
          group_by_cols_[group_index]);
    }));
  }

  // Initialize the Tuple Map
  global_tuple_map = new TupleMap();
  global_tuple_map->reserve(64);
}

void HashAggregate::ComputeAggregates(Task *ctx) {
  // TODO: Only support one column aggregation for now.
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task *internal) {
        // Initialize the agg_col_ variable.
        auto table = aggregate_refs_[0].col_ref.table;
        auto col_name = aggregate_refs_[0].col_ref.col_name;
        if (aggregate_refs_[0].expr_ref == nullptr) {
          agg_lazy_table_ = prev_result_->get_table(table);
          agg_lazy_table_.MaterializeColumn(internal, col_name, agg_col_);
        } else {
          // For expression case, create expression object and initialize
          expression_ = std::make_shared<Expression>(
              prev_result_, aggregate_refs_[0].expr_ref);
          expression_->Initialize(internal);
        }
      }),
      CreateLambdaTask([this](Task *internal) {
        // Partition the chunks.

        // Initialize the aggregate_col_data_?
        int32_t num_chunks;
        if (expression_ != nullptr) {
          num_chunks = expression_->num_chunks();
        } else {
          auto agg_col = agg_col_.chunked_array();
          num_chunks = agg_col->num_chunks();
          // TODO: Maybe this is unnecessary
          aggregate_col_data_.resize(num_chunks);
          for (std::size_t chunk_index = 0; chunk_index < num_chunks;
               ++chunk_index) {
            auto val =
                agg_col->chunk(chunk_index)->data()->GetValues<int64_t>(1, 0);
            aggregate_col_data_[chunk_index] = val;
          }
        }

        // Distribute tasks to perform the first phase of hash aggregate.
        std::size_t totalThreads = std::thread::hardware_concurrency();
        // TODO: Initialize the strategy at the Aggregate init time.
        // TODO: Make strategy control the hash function.
        strategy = HashAggregateStrategy(totalThreads, num_chunks);
        auto num_tasks = strategy.suggestedNumTasks();
        InitializeLocalHashTables();
      }),
      CreateLambdaTask([this](Task *internal) {
        // First Phase Aggregate
        auto num_tasks = strategy.suggestedNumTasks();
        std::size_t totalThreads = std::thread::hardware_concurrency();

        int32_t num_chunks = (expression_ == nullptr)
                                 ? agg_col_.chunked_array()->num_chunks()
                                 : expression_->num_chunks();
        for (size_t tid = 0; tid < num_tasks; ++tid) {
          int st, ed;
          std::tie(st, ed) = strategy.getChunkID(tid, totalThreads, num_chunks);
          if (st >= ed) {
            continue;
          }

          // First Phase Aggregation
          internal->spawnTask(CreateLambdaTask([this, tid, st, ed](Task *ctx) {
            //            printf("tid=%zu, st=%d, ed=%d\n", tid, st, ed);
            FirstPhaseAggregateChunks(ctx, tid, st, ed);
          }));
        }
      }),
      CreateLambdaTask([this](Task *internal) {
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
      })));
}

void HashAggregate::SecondPhaseAggregate(Task *internal) {
  auto kernel = aggregate_refs_[0].kernel;
  switch (kernel) {
    case SUM:
    case COUNT: {
      auto big_map = new HashMap();
      global_map = big_map;
      for (auto map : value_maps) {
        for (auto item : *map) {
          auto key = item.first;
          auto value = item.second;
          auto it = big_map->find(key);
          if (it == big_map->end()) {
            big_map->insert(std::make_pair(key, value));
          } else {
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
        for (auto ct : *cmap) {
          auto key = ct.first;
          auto count = ct.second;
          auto vt = vmap->find(key);
          if (vt == vmap->end()) {
            std::cerr << "Failed to find the key in value table: " << key
                      << std::endl;
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
      for (auto ct : *big_count_map) {
        auto key = ct.first;
        auto count = ct.second;
        auto vt = big_value_map->find(key);
        if (vt == big_value_map->end()) {
          std::cerr << "Failed to find the key in value table: " << key
                    << std::endl;
          throw std::exception();
        }
        auto value = vt->second;

        auto result = (double)value / (double)count;
        big_final_map->insert({key, result});
      }

      break;
    }
  }

  for (auto map : tuple_maps) {
    for (auto item : *map) {
      auto key = item.first;
      auto value = item.second;
      auto it = global_tuple_map->find(key);
      if (it == global_tuple_map->end()) {
        global_tuple_map->insert(std::make_pair(key, value));
      }
    }
  }
}

std::vector<std::shared_ptr<arrow::ArrayBuilder>>
HashAggregate::CreateGroupBuilderVector() {
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
  for (auto &field : group_type_->fields()) {
    auto builder = getBuilder(field->type());
    group_builders.push_back(builder);
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
    AggregateKernel kernel, const std::string &agg_col_name) {
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

void HashAggregate::FirstPhaseAggregateChunks(Task *internal, size_t tid,
                                              int st, int ed) {
  for (int i = st; i < ed; i++) {
    FirstPhaseAggregateChunk_(internal, tid, i);
  }
}

void HashAggregate::FirstPhaseAggregateChunk_(Task *internal, size_t tid,
                                              int chunk_index) {
  // if expression not null then evaluate the expression to get the result
  auto agg_chunk =
      (expression_ != nullptr)
          ? expression_->Evaluate(internal, chunk_index).make_array()
          : agg_col_.chunked_array()->chunk(chunk_index);
  auto chunk_size = agg_chunk->length();

  auto agg_type =
      (expression_ != nullptr) ? agg_chunk->type() : agg_col_.type();
  auto agg_type_id = agg_type->id();

  auto kernel = aggregate_refs_[0].kernel;

  // Retrieve the proper maps for aggregate
  HashMap *count_map = nullptr;
  HashMap *value_map = nullptr;
  TupleMap *tuple_map = nullptr;
  if (kernel == SUM || kernel == COUNT) {
    value_map = value_maps.at(tid);
  } else if (kernel == MEAN) {
    count_map = count_maps.at(tid);
    value_map = value_maps.at(tid);
  }
  tuple_map = tuple_maps.at(tid);

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
    for (const auto &group_by : group_by_cols_) {
      auto group_by_chunk = group_by.chunked_array()->chunk(chunk_index);

      auto group_by_type = group_by.type();

      // Assign next_key.
      hash_t next_key = 0;
      auto get_hash_key = [&]<typename T>(T *ptr_) {
        if constexpr (arrow::is_primitive_ctype<T>::value) {
          using ArrayType = ArrowGetArrayType<T>;
          using CType = ArrowGetCType<T>;

          auto col = std::static_pointer_cast<ArrayType>(group_by_chunk);
          CType val = col->Value(item_index);
          next_key = std::hash<CType>{}(val);
          return;
        }
        if constexpr (arrow::is_base_binary_type<T>::value ||
                      arrow::is_fixed_size_binary_type<T>::value) {
          using ArrayType = ArrowGetArrayType<T>;
          auto col = std::static_pointer_cast<ArrayType>(group_by_chunk);
          auto val = col->GetString(item_index);
          next_key = std::hash<std::string>{}(val);
          return;
        }
        /* TODO: Hash Key throw type list:
         * null fixed_size_binary date32 date64 time32 time64 timestamp
         * day_time_interval month_interval duration decimal struct list
         * large_list fixed_size_list map dense_union sparse_union dictionary
         * extension
         */
        throw std::runtime_error(std::string("Unhashable type: ") +
                                 T::type_name());
      };
      type_switcher(group_by_type, get_hash_key);

      key = HashAggregateStrategy::HashCombine(key, next_key);
    }
    agg_item_key = key;

    // TODO: Without a unique operator, we don't need to calculate
    //  the String type hash code.
    switch (agg_type_id) {
      case arrow::Type::STRING: {
        // SUM, COUNT, MEAN
        assert(kernel != MEAN);
        assert(kernel != SUM);
        agg_item_value = 1;
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        // TODO: Not very sure what the FIXED_SIZE_BINARY can do...
        assert(kernel != MEAN);
        assert(kernel != SUM);
        agg_item_value = 1;
        break;
      }
      case arrow::Type::INT64: {
        // SUM, COUNT, MEAN
        switch (kernel) {
          case SUM:
          case MEAN: {
            auto agg_col =
                std::static_pointer_cast<arrow::Int64Array>(agg_chunk);
            agg_item_value = agg_col->Value(item_index);
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

    // TODO: Optimize the hash key access pattern.
    switch (kernel) {
      case SUM: {
        auto it = value_map->find(agg_item_key);
        if (it == value_map->end()) {
          value_map->insert(std::make_pair(agg_item_key, agg_item_value));
        } else {
          it->second = agg_item_value + it->second;
        }
        break;
      }

      case COUNT: {
        auto it = value_map->find(agg_item_key);
        if (it == value_map->end()) {
          value_map->insert(std::make_pair(agg_item_key, 1));
        } else {
          it->second = 1 + it->second;
        }
        break;
      }

      case MEAN: {
        auto it = value_map->find(agg_item_key);
        auto jt = count_map->find(agg_item_key);
        if (it == value_map->end()) {
          value_map->insert(std::make_pair(agg_item_key, agg_item_value));
          count_map->insert(std::make_pair(agg_item_key, 1));
        } else {
          it->second = agg_item_value + it->second;
          jt->second = 1 + jt->second;
        }
        break;
      }
      default: {
        std::cerr << "Not supported aggregate kernel." << std::endl;
        break;
      }
    }

    auto it = tuple_map->find(agg_item_key);
    if (it == tuple_map->end()) {
      // Only insert the tuple when its not shown.
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
        auto tuple_map = new TupleMap();
        value_maps.push_back(value_map);
        count_maps.push_back(count_map);
      }
      break;
    }
  }

  // Initialize tuple maps for the tasks
  for (size_t tid = 0; tid < num_tasks; ++tid) {
    auto tuple_map = new TupleMap();
    tuple_maps.push_back(tuple_map);
  }
}

void HashAggregate::Finish(Task *ctx) {
  // Finish the aggregate and output the column.
  //  Procedure:
  //    1. Construct the resulting group-by columns
  //    2. Construct the resulting aggregate table
  //    3. Construct the final table for output.

  // 1. Construct the resulting group-by columns
  std::vector<std::shared_ptr<arrow::Array>> group_by_arrays;
  group_by_arrays.reserve(group_by_cols_.size());

  for (int i = 0; i < group_by_cols_.size(); i++) {
    auto group_by = group_by_cols_.at(i);
    auto group_by_type = group_by.type();

    // TODO: The following step assume the iteration of tuple map
    //        is not going to mutate.

    auto data_type = group_by_type;

    auto normal_value_accessor = [](const auto &chunk, const auto item_index) {
      return chunk->Value(item_index);
    };
    auto string_value_accessor = [](const auto &chunk, const auto item_index) {
      return chunk->GetString(item_index);
    };

    auto core_fn = [&]<typename T>(T *, auto value_accessor) {
      using ArrayType = ArrowGetArrayType<T>;
      using BuilderType = ArrowGetBuilderType<T>;

      auto builder =
          std::reinterpret_pointer_cast<BuilderType>(getBuilder(data_type));

      for (auto const it : *global_tuple_map) {
        int chunk_id, item_index;
        auto hash_key = it.first;
        std::tie(chunk_id, item_index) = it.second;
        auto raw_chunk = group_by.chunks().at(chunk_id);
        auto chunk = std::static_pointer_cast<ArrayType>(raw_chunk);
        // TODO: Optimize this to make it std::move()?
        // TODO: (C++20) Perfect forward this to the builder.
        // TODO: Try to use the UnsafeAppend here,
        //  but encounter an error.
        //  builder.UnsafeAppend(value);
        builder->Append(value_accessor(chunk, item_index));
      }
      std::shared_ptr<arrow::Array> array;
      arrow::Status st = builder->Finish(&array);
      // TODO: Use the throw function.
      if (!st.ok()) {
        throw std::runtime_error("Building the array for " + std::to_string(i) +
                                 "th groupby column failed.");
      }
      group_by_arrays.push_back(array);
    };

    // TODO: (Optimize) Can we indicate the accessor and collapse the code?
    // TODO: Test on other value type arrays.
    auto finish_handler = [&]<typename T>(T *ptr_) {
      if constexpr (arrow::is_string_type<T>::value) {
        return core_fn(ptr_, string_value_accessor);

      } else if constexpr (arrow::is_number_type<T>::value) {
        return core_fn(ptr_, normal_value_accessor);

      } else {
        throw std::logic_error(
            "HashAggregate::Finish(). Not supported aggregate group by column "
            "type: " +
            data_type->ToString());
      }
    };
    type_switcher(data_type, finish_handler);
  }

  // 2. Construct the resulting aggregate table
  auto kernel = aggregate_refs_[0].kernel;
  std::shared_ptr<arrow::Array> agg_array;
  // TODO: Make a function that generalize the casting
  //  of builder for sum, count and mean.
  switch (kernel) {
    case SUM:
    case COUNT: {
      auto map = static_cast<HashMap *>(global_map);
      arrow::Int64Builder builder;
      for (auto const it : *global_tuple_map) {
        auto hash_key = it.first;
        auto value = map->find(hash_key)->second;
        builder.Append(value);
        // UnsafeAppend
      }
      arrow::Status st = builder.Finish(&agg_array);
      if (!st.ok()) {
        std::cerr << "Building the array for aggregate column failed."
                  << std::endl;
        exit(1);
      }
      break;
    }
    case MEAN: {
      auto map = static_cast<MeanHashMap *>(global_map);
      arrow::DoubleBuilder builder;
      for (auto const it : *global_tuple_map) {
        auto hash_key = it.first;
        auto value = map->find(hash_key)->second;
        builder.Append(value);
      }
      arrow::Status st = builder.Finish(&agg_array);
      if (!st.ok()) {
        std::cerr << "Building the array for aggregate column failed."
                  << std::endl;
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
  for (auto &group_by_array : group_by_arrays) {
    auto pt = group_by_array->data();
    groups.emplace_back(pt);
  }
  aggregates.value = agg_array->data();

  SortResult(groups, aggregates);

  // 4. Construct the final table for output.
  std::vector<std::shared_ptr<arrow::ArrayData>> output_table_data;
  output_table_data.reserve(group_by_arrays.size() + 1);
  for (auto &group_values : groups) {
    output_table_data.push_back(group_values.make_array()->data());
  }
  output_table_data.push_back(aggregates.make_array()->data());

  output_table_->InsertRecords(output_table_data);
  output_result_->append(output_table_);
}

void HashAggregate::SortResult(std::vector<arrow::Datum> &groups,
                               arrow::Datum &aggregates) {
  // The columns in the GROUP BY and ORDER BY clause may not directly correspond
  // to the same column, e.g we may have
  // GROUP BY R.a, R.b
  // ORDER BY R.b, R.a
  // order_by_group[i] is the index at which the ith ORDER BY column appears in
  // the GROUP BY clause. In this example, order_to_group = {1, 0}
  std::vector<int> order_to_group;

  for (auto &order_by_ref : order_by_refs_) {
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
  // TODO: Why do we handle order by in a reversed way?
  for (int i = order_by_refs_.size() - 1; i >= 0; i--) {
    auto order_ref = order_by_refs_[i];
    // A nullptr indicates that we are sorting by the aggregate column
    // TODO(nicholas): better way to indicate we want to sort the aggregate?
    if (order_ref.table == nullptr) {
      status = arrow::compute::SortIndices(*aggregates.make_array())
                   .Value(&sorted_indices);
      evaluate_status(status, __FUNCTION__, __LINE__);
    } else {
      auto group = groups[order_to_group[i]];
      status = arrow::compute::SortIndices(*group.make_array())
                   .Value(&sorted_indices);
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
    status = arrow::compute::Take(aggregates, sorted_indices, take_options)
                 .Value(&aggregates);
    evaluate_status(status, __FUNCTION__, __LINE__);

    for (auto &group : groups) {
      status = arrow::compute::Take(group, sorted_indices, take_options)
                   .Value(&group);
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
  }
}

}  // namespace hustle::operators