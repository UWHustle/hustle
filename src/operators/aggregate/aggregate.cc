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

#include "operators/aggregate/aggregate.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <utility>

#include "storage/utils/util.h"
#include "type/type_helper.h"

#define AGGREGATE_OUTPUT_TABLE "aggregate"

namespace hustle::operators {

Aggregate::Aggregate(const std::size_t query_id,
                     OperatorResult::OpResultPtr prev_result,
                     OperatorResult::OpResultPtr output_result,
                     std::vector<AggregateReference> aggregate_refs,
                     std::vector<ColumnReference> group_by_refs,
                     std::vector<ColumnReference> order_by_refs)
    : Aggregate(query_id, prev_result, output_result, aggregate_refs,
                group_by_refs, order_by_refs,
                std::make_shared<OperatorOptions>()) {}

Aggregate::Aggregate(const std::size_t query_id,
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

void Aggregate::execute(Task* ctx) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task* internal) { Initialize(internal); }),
      CreateLambdaTask([this](Task* internal) { ComputeAggregates(internal); }),
      CreateLambdaTask([this](Task* internal) { Finish(); })));
}

void Aggregate::Initialize(Task* ctx) {
  std::vector<std::shared_ptr<Context>> contexts;
  contexts.reserve(group_by_refs_.size());
  for (std::size_t i = 0; i < group_by_refs_.size(); ++i) {
    contexts.push_back(std::make_shared<Context>());
  }

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask(
          [this](Task* internal) { InitializeVariables(internal); }),
      CreateLambdaTask([this, contexts](Task* internal) {
        // Fetch unique values for all Group By columns.
        for (std::size_t group_index = 0; group_index < group_by_refs_.size();
             group_index++) {
          internal->spawnTask(CreateTaskChain(
              CreateLambdaTask([this, group_index](Task* internal) {
                InitializeGroupByColumn(internal, group_index);
              }),
              CreateLambdaTask([this, group_index] {
                unique_values_[group_index] =
                    ComputeUniqueValues(group_by_refs_[group_index])
                        .make_array();
              }),
              CreateLambdaTask([this, group_index, contexts](Task* internal) {
                contexts[group_index]->match(internal,
                                             group_by_cols_[group_index],
                                             unique_values_[group_index]);
              }),
              CreateLambdaTask([this, contexts, group_index] {
                unique_values_map_[group_index] =
                    contexts[group_index]->out_.chunked_array();
              })));
        }
      })));
}

void Aggregate::InitializeVariables(Task* ctx) {
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
  exp_result_finished_ = false;
  exp_result_builder_ = std::make_shared<arrow::Int64Builder>();

  // Initialize output table and its schema. group_type_ must be initialized
  // beforehand.
  std::shared_ptr<arrow::Schema> out_schema =
      OutputSchema(aggregate_refs_[0].kernel, aggregate_refs_[0].agg_name);
  output_table_ =
      std::make_shared<DBTable>(AGGREGATE_OUTPUT_TABLE, out_schema, BLOCK_SIZE);

  unique_values_.resize(group_by_refs_.size());
  group_by_cols_.resize(group_by_refs_.size());

  for (auto& group_by : group_by_refs_) {
    group_by_tables_.push_back(prev_result_->get_table(group_by.table));
  }
  unique_values_map_.resize(group_by_refs_.size());
}

void Aggregate::InitializeGroupByColumn(Task* ctx, std::size_t group_index) {
  std::scoped_lock<std::mutex> lock(mutex_);
  group_by_index_map_.emplace(group_by_refs_[group_index].col_name,
                              group_index);
  group_by_tables_[group_index].MaterializeColumn(
      ctx, group_by_refs_[group_index].col_name, group_by_cols_[group_index]);
}

void Aggregate::InitializeGroupFilters(Task* ctx) {
  if (group_type_->num_fields() == 0) {
    return;
  }

  ctx->spawnLambdaTask([this](Task* internal) {
    auto num_fields = group_by_cols_.size();

    std::vector<arrow::Type::type> field_types;
    field_types.reserve(num_fields);

    for (std::size_t field_index = 0; field_index < num_fields; ++field_index) {
      auto field_unique_values = unique_values_[field_index]->data();
      field_types.push_back(field_unique_values->type->id());
    }

    auto agg_col = agg_col_.chunked_array();
    auto num_chunks = agg_col->num_chunks();
    aggregate_col_data_.resize(num_chunks);

    for (std::size_t chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
      aggregate_col_data_[chunk_index] =
          agg_col->chunk(chunk_index)->data()->GetValues<int64_t>(1, 0);
    }

    std::size_t batch_size = num_chunks / (std::thread::hardware_concurrency() *
                                           options_->get_parallel_factor());
    if (batch_size == 0) batch_size = num_chunks;
    std::size_t num_batches = 1 + ((num_aggs_ - 1) / batch_size);

    for (std::size_t batch_index = 0; batch_index < num_batches;
         batch_index++) {
      // Each task gets the filter for one block and stores it in filter_vector
      internal->spawnLambdaTask([this, num_chunks, num_fields, batch_index,
                                 batch_size](Task* internal) {
        std::vector<const uint32_t*> group_map;
        group_map.resize(num_fields);

        int base_index = batch_index * batch_size;
        for (std::size_t block_index = base_index;
             block_index < base_index + batch_size && block_index < num_chunks;
             ++block_index) {
          BlockScan(group_map, block_index);
        }
      });
    }
  });
}

std::vector<std::shared_ptr<arrow::ArrayBuilder>>
Aggregate::CreateGroupBuilderVector() {
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> group_builders;
  for (auto& field : group_type_->fields()) {
    std::shared_ptr<arrow::ArrayBuilder> builder = getBuilder(field->type());
    group_builders.push_back(builder);
  }
  return group_builders;
}

std::shared_ptr<arrow::ArrayBuilder> Aggregate::CreateAggregateBuilder(
    AggregateKernel kernel) {
  std::shared_ptr<arrow::ArrayBuilder> aggregate_builder;
  switch (kernel) {
    case MEAN: {
      aggregate_builder = std::make_shared<arrow::DoubleBuilder>();
      break;
    }
    case SUM:
    case COUNT: {
      aggregate_builder = std::make_shared<arrow::Int64Builder>();
      break;
    }
  }
  return aggregate_builder;
}

std::shared_ptr<arrow::Schema> Aggregate::OutputSchema(
    AggregateKernel kernel, const std::string& agg_col_name) {
  arrow::Status status;
  arrow::SchemaBuilder schema_builder;

  if (group_type_ != nullptr) {
    status = schema_builder.AddFields(group_type_->fields());
    evaluate_status(status, __FUNCTION__, __LINE__);
  }
  switch (kernel) {
    case MEAN: {
      status =
          schema_builder.AddField(arrow::field(agg_col_name, arrow::float64()));
      evaluate_status(status, __FUNCTION__, __LINE__);
      break;
    }

    case SUM:
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

void Aggregate::BlockScan(std::vector<const uint32_t*>& group_map,
                          int chunk_index) {
  auto chunk_length = agg_col_.chunked_array()->chunk(chunk_index)->length();
  auto num_fields = group_by_refs_.size();
  auto agg_col_chunk_data = aggregate_col_data_[chunk_index];

  for (std::size_t field_index = 0; field_index < num_fields; ++field_index) {
    group_map[field_index] = unique_values_map_[field_index]
                                 .chunked_array()
                                 ->chunk(chunk_index)
                                 ->data()
                                 ->GetValues<uint32_t>(1, 0);
  }

  for (std::size_t row_index = 0; row_index < chunk_length; ++row_index) {
    auto key = 0;
    for (std::size_t group_id = 0; group_id < num_fields; ++group_id) {
      // @TODO(nicholas): save time by precomputing 10^(k+1)
      key += group_map[group_id][row_index] * pow(10, group_id);
    }

    auto agg_index = group_agg_index_map_[key];
    aggregate_data_[agg_index] += agg_col_chunk_data[row_index];
  }
}

void Aggregate::InsertGroupColumns(std::vector<int> group_id, int agg_index) {
  arrow::Status status;
  // Loop over columns in group builder, and append one of its unique values to
  // its builder.
  for (std::size_t i = 0; i < group_type_->num_fields(); ++i) {
    // Handle the insertion of record.
    auto data_type = group_type_->field(i)->type();
    auto insert_handler = [&, this]<typename T>(T* ptr_) {
      // TODO: Add support for other types.
      //  Right now we only support types that have builder and array
      //  in the type trait. It should be obvious that we can:
      //  - [1] Get builder from field spec
      //  - [2] Get array for most cases (except for Dict and Extension).
      constexpr bool is_supported_type = std::conjunction_v<
          has_builder_type<T>, has_array_type<T>,
          isNotOneOf<T, arrow::ExtensionType, arrow::DictionaryType>>;

      if constexpr (is_supported_type) {
        using BuilderType = ArrowGetBuilderType<T>;
        using ArrayType = ArrowGetArrayType<T>;

        if constexpr (arrow::is_number_type<T>::value) {
          // Downcast the column's builder
          auto col_group_builder = (BuilderType*)(group_builder_->child(i));
          // Downcast the column's unique_values
          auto col_unique_values =
              std::static_pointer_cast<ArrayType>(unique_values_[i]);
          status =
              col_group_builder->Append(col_unique_values->Value(group_id[i]));
          evaluate_status(status, __FUNCTION__, __LINE__);
          return;

        } else if constexpr (arrow::is_string_type<T>::value) {
          // Downcast the column's builder
          auto col_group_builder = (BuilderType*)(group_builder_->child(i));
          // Downcast the column's unique_values
          auto col_unique_values =
              std::static_pointer_cast<ArrayType>(unique_values_[i]);
          status = col_group_builder->Append(
              col_unique_values->GetString(group_id[i]));
          evaluate_status(status, __FUNCTION__, __LINE__);
          return;
        }
      } else {
        throw std::runtime_error("Cannot insert unsupported aggregate type:" +
                                 data_type->ToString());
      }
    };
    type_switcher(data_type, insert_handler);
  }
  // StructBuilder does not automatically update its length when we append to
  // its children. We must do this manually.
  status = group_builder_->Append(true);
  evaluate_status(status, __FUNCTION__, __LINE__);
}

void Aggregate::InsertGroupAggregate(int agg_index) {
  auto aggregate_builder_casted =
      std::static_pointer_cast<arrow::Int64Builder>(aggregate_builder_);
  arrow::Status status =
      aggregate_builder_casted->Append(aggregate_data_[agg_index]);
  evaluate_status(status, __FUNCTION__, __LINE__);
}

arrow::Datum Aggregate::ComputeUniqueValues(const ColumnReference& group_ref) {
  auto group_by_col = group_by_cols_[group_by_index_map_[group_ref.col_name]];
  // Get the unique values in group_by_col
  arrow::Datum unique_values;
  arrow::Status status =
      arrow::compute::Unique(group_by_col).Value(&unique_values);
  evaluate_status(status, __FUNCTION__, __LINE__);

  return unique_values;
}

arrow::Datum Aggregate::ComputeAggregate(AggregateKernel kernel,
                                         const arrow::Datum& aggregate_col) {
  arrow::Status status;
  arrow::Datum out_aggregate;

  switch (kernel) {
    case SUM: {
      status = arrow::compute::Sum(aggregate_col).Value(&out_aggregate);
      break;
    }
    case COUNT: {
      status = arrow::compute::Count(aggregate_col).Value(&out_aggregate);
      break;
    }
      // Note that Mean outputs a DOUBLE
    case MEAN: {
      status = arrow::compute::Mean(aggregate_col).Value(&out_aggregate);
      break;
    }
  }
  evaluate_status(status, __FUNCTION__, __LINE__);
  return out_aggregate;
}

void Aggregate::ComputeGroupAggregate(Task* ctx, std::size_t agg_index,
                                      const std::vector<int>& group_id,
                                      const arrow::Datum agg_col) {
  if (num_aggs_ == 1) {
    auto aggregate =
        ComputeAggregate(aggregate_refs_[0].kernel, agg_col.chunked_array());
    aggregate_data_[agg_index] =
        std::static_pointer_cast<arrow::Int64Scalar>(aggregate.scalar())->value;
  }
  if (aggregate_data_[agg_index] > 0) {
    // Compute the aggregate over the filtered agg_col
    // Acquire mutex_ so that groups are correctly associated with
    // their corresponding aggregates
    std::unique_lock<std::mutex> lock(mutex_);
    InsertGroupAggregate(agg_index);
    InsertGroupColumns(group_id, agg_index);
  }
}

void Aggregate::ComputeAggregates(Task* ctx) {
  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([this](Task* internal) {
        // TODO(nicholas): For now, we only perform one aggregate.
        auto table = aggregate_refs_[0].col_ref.table;
        auto col_name = aggregate_refs_[0].col_ref.col_name;
        agg_lazy_table_ = prev_result_->get_table(table);
        agg_lazy_table_.MaterializeColumn(internal, col_name, agg_col_);
      }),
      CreateLambdaTask([this](Task* internal) {
        // Initialize the slots to hold the current iteration value for each
        // depth
        int num_group_columns = group_type_->num_fields();
        int max_unique_values[num_group_columns];
        std::vector<int> group_id(num_group_columns);

        // DYNAMIC DEPTH NESTED LOOP:
        // We must handle an arbitrary number of GROUP BY columns. We need a
        // dynamic nested loop to iterate over all possible groups. If maxes =
        // {2, 3} (i.e. we have two group by columns which have 2 and 3 unique
        // values, respectively), then group_id takes on the following values at
        // each iteration:
        //
        // {0, 0}
        // {0, 1}
        // {0, 2}
        // {1, 0}
        // {1, 1}
        // {1, 2}

        // initialize group_id = {0, 0, ..., 0} and initialize maxes[i] to the
        // number of unique values in group by column i.
        num_aggs_ = 1;
        for (std::size_t i = 0; i < num_group_columns; ++i) {
          group_id[i] = 0;
          max_unique_values[i] = unique_values_[i]->length();
          num_aggs_ *= max_unique_values[i];
        }

        group_agg_index_map_.reserve(num_aggs_);
        group_id_vec_.resize(num_aggs_);
        aggregate_data_ = (std::atomic<int64_t>*)calloc(
            num_aggs_, sizeof(std::atomic<int64_t>));

        int agg_index = 0;
        int index = num_group_columns - 1;  // loop index

        bool exit = false;
        while (!exit) {
          /*
           * 1. COPY the current group_id to group_id_vec_[agg_index]
           * 2. CREATE hash key of the group_id
           * 3. INSERT the (key, agg_index) pair into group_agg_index_map_.
           * 4. INCREMENT the group_id using dynamic nested depth loop.
           * */

          // 1. COPY group_id
          // LOOP BODY START
          group_id_vec_[agg_index] = group_id;
          // 2. CREATE hash key
          auto key = 0;
          for (std::size_t k = 0; k < group_id.size(); ++k) {
            key += group_id[k] * pow(10, k);
          }
          // 3. INSERT hash key map to agg_index
          group_agg_index_map_[key] = agg_index++;
          // LOOP BODY END

          if (num_group_columns == 0) {
            break;  // Only execute the loop once if there are no group bys
          }

          // 4. INCREMENT group_id
          group_id[num_group_columns - 1]++;
          while (group_id[index] == max_unique_values[index]) {
            if (index == 0) {
              exit = true;
              break;
            }
            group_id[index--] = 0;
            ++group_id[index];
          }
          index = num_group_columns - 1;
        }
        InitializeGroupFilters(internal);
      }),
      CreateLambdaTask([this](Task* internal) {
        std::size_t batch_size =
            num_aggs_ / std::thread::hardware_concurrency() / 2;
        if (batch_size == 0) batch_size = num_aggs_;
        std::size_t num_batches = 1 + ((num_aggs_ - 1) / batch_size);

        for (std::size_t batch_index = 0; batch_index < num_batches;
             ++batch_index) {
          // Each task gets the filter for one block and stores it in
          // filter_vector
          internal->spawnLambdaTask([this, batch_index,
                                     batch_size](Task* internal) {
            int base_index = batch_index * batch_size;
            for (std::size_t agg_index = base_index;
                 agg_index < base_index + batch_size && agg_index < num_aggs_;
                 ++agg_index) {
              ComputeGroupAggregate(internal, agg_index,
                                    group_id_vec_[agg_index], agg_col_);
            }
          });
        }
      })));
}

void Aggregate::SortResult(std::vector<arrow::Datum>& groups,
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
  // TODO: Use a reversed iterator here instead of the index approach.
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

    for (auto& group : groups) {
      status = arrow::compute::Take(group, sorted_indices, take_options)
                   .Value(&group);
      evaluate_status(status, __FUNCTION__, __LINE__);
    }
  }
}

void Aggregate::Clear() {
  num_aggs_ = 0;

  prev_result_.reset();
  output_result_.reset();
  output_table_.reset();

  aggregate_col_data_.clear();
  aggregate_refs_.clear();
  group_by_refs_.clear();
  order_by_refs_.clear();
  group_by_cols_.clear();
  group_agg_index_map_.clear();
  unique_values_map_.clear();
  unique_values_.clear();
}

void Aggregate::Finish() {
  arrow::Status status;

  // Create Arrow Arrays from the ArrayBuilders
  std::shared_ptr<arrow::StructArray> groups_temp;
  status = group_builder_->Finish(&groups_temp);
  evaluate_status(status, __FUNCTION__, __LINE__);

  std::vector<arrow::Datum> groups;
  arrow::Datum aggregates;

  groups.reserve(groups_temp->num_fields());
  for (std::size_t field_index = 0; field_index < groups_temp->num_fields();
       ++field_index) {
    groups.emplace_back(groups_temp->field(field_index));
  }
  std::shared_ptr<arrow::Array> agg_temp;
  status = aggregate_builder_->Finish(&agg_temp);
  evaluate_status(status, __FUNCTION__, __LINE__);
  aggregates.value = agg_temp->data();

  // Sort the output according to the ORDER BY clause
  SortResult(groups, aggregates);

  // Add the sorted data to the output table, and wrap the output table in
  // the output OperatorResult.
  std::vector<std::shared_ptr<arrow::ArrayData>> output_table_data;
  for (auto& group_values : groups) {
    output_table_data.push_back(group_values.make_array()->data());
  }
  output_table_data.push_back(aggregates.make_array()->data());

  output_table_->InsertRecords(output_table_data);
  output_result_->append(output_table_);

  free(aggregate_data_);
}

}  // namespace hustle::operators