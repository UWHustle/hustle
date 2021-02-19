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

#include "operators/select/select.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <utility>

#include "storage/metadata_wrapper.h"
#include "storage/table.h"
#include "storage/util.h"
#include "utils/bit_utils.h"

namespace hustle {
namespace operators {

Select::Select(const std::size_t query_id, DBTable::TablePtr table,
               OperatorResult::OpResultPtr prev_result,
               OperatorResult::OpResultPtr output_result,
               std::shared_ptr<PredicateTree> tree)
    : Select(query_id, table, prev_result, output_result, tree,
             std::make_shared<OperatorOptions>()) {}

Select::Select(const std::size_t query_id, DBTable::TablePtr table,
               OperatorResult::OpResultPtr prev_result,
               OperatorResult::OpResultPtr output_result,
               std::shared_ptr<PredicateTree> tree,
               std::shared_ptr<OperatorOptions> options)
    : Operator(query_id, options),
      table_(table),
      output_result_(output_result),
      tree_(tree) {
  filters_.resize(table_->get_num_blocks());
}

void Select::execute(Task *ctx) {
  ctx->spawnTask(CreateTaskChain(
      // Task 1: perform selection on all blocks
      CreateLambdaTask([this](Task *internal) {
        table_->ForEachBatch([&](auto batch_index, auto batch_size) {
          internal->spawnLambdaTask([this, batch_index, batch_size]() {
            size_t base_index = batch_index * batch_size;
            size_t batch_limit =
                std::min((base_index + batch_size), table_->get_num_blocks());
            for (size_t block_index = base_index; block_index < batch_limit;
                 block_index++) {
              this->ExecuteBlock(block_index);
            }
          });
        });
      }),
      // Task 2: create the output result
      CreateLambdaTask([this](Task *internal) {
        auto chunked_filter = std::make_shared<arrow::ChunkedArray>(filters_);
        auto lazy_table = std::make_shared<LazyTable>(
            table_, chunked_filter, arrow::Datum(), arrow::Datum());
        output_result_->append(lazy_table);
      })));
}

void Select::ExecuteBlock(int block_index) {
  auto block = table_->get_block(block_index);

  std::shared_ptr<arrow::Buffer> filter_buffer;
  auto status =
      arrow::AllocateBitmap(block->get_num_rows()).Value(&filter_buffer);
  evaluate_status(status, __FUNCTION__, __LINE__);

  auto block_filter = this->Filter(block, tree_->root_);
  filters_[block_index] = block_filter.make_array();
}

arrow::Datum Select::Filter(const std::shared_ptr<Block> &block,
                            const std::shared_ptr<Node> &node) {
  arrow::Status status;
  if (node->is_leaf()) {
    return Filter(block, node->predicate_);
  }

  auto left_child_filter = Filter(block, node->left_child_);
  auto right_child_filter = Filter(block, node->right_child_);
  arrow::Datum block_filter;

  auto left_data = left_child_filter.array()->GetMutableValues<uint8_t>(1);
  auto right_data = right_child_filter.array()->GetValues<uint8_t>(1);

  auto num_bytes = 1 + ((left_child_filter.length() - 1) / CHAR_BIT);
  switch (node->connective_) {
    case AND: {
      for (int i = 0; i < num_bytes; ++i) {
        left_data[i] = left_data[i] & right_data[i];
      }
      break;
    }
    case OR: {
      for (int i = 0; i < num_bytes; ++i) {
        left_data[i] = left_data[i] | right_data[i];
      }
      break;
    }
    case NONE: {
      break;
    }
  }
  return left_child_filter;
}

arrow::Datum Select::Filter(const std::shared_ptr<Block> &block,
                            const std::shared_ptr<Predicate> &predicate) {
  switch (predicate->value_.type()->id()) {
    case arrow::Type::UINT8: {
      uint8_t val = std::static_pointer_cast<arrow::UInt8Scalar>(
                        predicate->value_.scalar())
                        ->value;
      auto datum_val = predicate->value_;

      switch (predicate->comparator_) {
        case arrow::compute::CompareOperator::LESS: {
          return Filter<uint8_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::less());
        }
        case arrow::compute::CompareOperator::LESS_EQUAL: {
          return Filter<uint8_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::less_equal());
        }
        case arrow::compute::CompareOperator::GREATER: {
          return Filter<uint8_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::greater());
        }
        case arrow::compute::CompareOperator::GREATER_EQUAL: {
          return Filter<uint8_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::greater_equal());
        }
        case arrow::compute::CompareOperator::EQUAL: {
          return Filter<uint8_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::equal_to());
        }
        // TODO(nicholas): placeholder for BETWEEN
        case arrow::compute::CompareOperator::NOT_EQUAL: {
          auto num_rows = block->get_num_rows();
          auto col_data =
              block->get_column_by_name(predicate->col_ref_.col_name)
                  ->data()
                  ->GetValues<uint8_t>(1);

          auto lo = std::static_pointer_cast<arrow::UInt8Scalar>(
                        predicate->value_.scalar())
                        ->value;
          auto hi = std::static_pointer_cast<arrow::UInt8Scalar>(
                        predicate->value2_.scalar())
                        ->value;
          auto diff = hi - lo;

          std::shared_ptr<arrow::Buffer> buffer;
          auto status = arrow::AllocateBuffer(num_rows).Value(&buffer);
          evaluate_status(status, __FUNCTION__, __LINE__);
          auto bytemap = buffer->mutable_data();

          auto f = [](uint8_t val, uint8_t diff) -> bool {
            return val <= diff;
          };
          for (uint32_t i = 0; i < num_rows; ++i) {
            bytemap[i] = f(col_data[i] - lo, diff);
          }

          std::shared_ptr<arrow::BooleanArray> out_filter;
          utils::pack(num_rows, bytemap, &out_filter);
          return out_filter;
        }
        default: {
          std::cerr << "No support for comparator" << std::endl;
        }
      }
      break;
    }
    case arrow::Type::INT64: {
      int64_t val = std::static_pointer_cast<arrow::Int64Scalar>(
                        predicate->value_.scalar())
                        ->value;
      auto datum_val = predicate->value_;

      switch (predicate->comparator_) {
        case arrow::compute::CompareOperator::LESS: {
          return Filter<int64_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::less());
          break;
        }
        case arrow::compute::CompareOperator::LESS_EQUAL: {
          return Filter<int64_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::less_equal());
          break;
        }
        case arrow::compute::CompareOperator::GREATER: {
          return Filter<int64_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::greater());
          break;
        }
        case arrow::compute::CompareOperator::GREATER_EQUAL: {
          return Filter<int64_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::greater_equal());
          break;
        }
        case arrow::compute::CompareOperator::EQUAL: {
          return Filter<int64_t>(block, predicate->col_ref_, datum_val, val,
                                 predicate->comparator_, std::equal_to());
          break;
        }
        case arrow::compute::CompareOperator::NOT_EQUAL: {
          auto num_rows = block->get_num_rows();
          auto col_data =
              block->get_column_by_name(predicate->col_ref_.col_name)
                  ->data()
                  ->GetValues<int64_t>(1);

          auto lo = std::static_pointer_cast<arrow::Int64Scalar>(
                        predicate->value_.scalar())
                        ->value;
          auto hi = std::static_pointer_cast<arrow::Int64Scalar>(
                        predicate->value2_.scalar())
                        ->value;
          auto diff = hi - lo;

          std::shared_ptr<arrow::Buffer> buffer;
          auto status = arrow::AllocateBuffer(num_rows).Value(&buffer);
          evaluate_status(status, __FUNCTION__, __LINE__);

          auto f = [](int64_t val, int64_t diff) -> bool {
            return val <= diff;
          };

          auto bytemap = buffer->mutable_data();
          for (uint32_t i = 0; i < num_rows; ++i) {
            bytemap[i] = f(col_data[i] - lo, diff);
          }

          std::shared_ptr<arrow::BooleanArray> out_filter;
          utils::pack(num_rows, bytemap, &out_filter);
          return out_filter;
        }
        default: {
          std::cerr << "No supprt for comparator" << std::endl;
        }
      }
      break;
    }

    default: {
      arrow::Status status;
      arrow::compute::CompareOptions compare_options(predicate->comparator_);
      arrow::Datum block_filter;

      auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);
      auto value = predicate->value_;

      status = arrow::compute::Compare(select_col, value, compare_options)
                   .Value(&block_filter);
      evaluate_status(status, __FUNCTION__, __LINE__);

      filters_[block->get_id()] = block_filter.make_array();
      return block_filter;
    }
  }
}

void Select::Clear() { filters_.clear(); }

template <typename T, typename Op>
arrow::Datum Select::Filter(const std::shared_ptr<Block> &block,
                            const ColumnReference &col_ref,
                            const arrow::Datum &arrow_val, const T &value,
                            arrow::compute::CompareOperator arrow_compare,
                            Op comparator) {
  std::shared_ptr<arrow::Buffer> buffer;
  arrow::Datum block_filter;
  auto num_rows = block->get_num_rows();
  auto status = arrow::AllocateBuffer(num_rows).Value(&buffer);
  evaluate_status(status, __FUNCTION__, __LINE__);
  auto bytemap = buffer->mutable_data();
  std::shared_ptr<arrow::BooleanArray> out_filter;

  if (block->IsMetadataCompatible()) {
    auto metadata_block = std::static_pointer_cast<MetadataEnabledBlock>(block);
    if (!metadata_block->SearchMetadata(col_ref.col_name, arrow_val,
                                        arrow_compare)) {
      memset(bytemap, 0, sizeof(&bytemap));
    } else {
      auto col_data =
          block->get_column_by_name(col_ref.col_name)->data()->GetValues<T>(1);
      for (uint32_t i = 0; i < num_rows; ++i) {
        bytemap[i] = comparator(col_data[i], value);
      }
    }
  } else {
    auto col_data =
        block->get_column_by_name(col_ref.col_name)->data()->GetValues<T>(1);
    for (uint32_t i = 0; i < num_rows; ++i) {
      bytemap[i] = comparator(col_data[i], value);
    }
  }
  utils::pack(num_rows, bytemap, &out_filter);
  return out_filter;
}

}  // namespace operators
}  // namespace hustle
