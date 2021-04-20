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

#include "storage/ma_block.h"
#include "storage/table.h"
#include "type/type_helper.h"
#include "utils/bit_utils.h"
#include "utils/arrow_utils.h"

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

void Select::Execute(Task *ctx, int32_t flags) {
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
  auto data_type = predicate->value_.type();

  // [1] Handle default types.
  // TODO: (Type Coverage) Any concerns over the default types?
  auto default_filter = [&]<typename T>(T *) -> arrow::Datum {
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
  };

  // [2] Handle numeric types.
  // TODO: There are a lot of assumptions over the type T.
  auto numeric_filter = [&]<typename T>(T *) -> arrow::Datum {
    using CType = ArrowGetCType<T>;
    using ScalarType = ArrowGetScalarType<T>;

    CType val =
        std::static_pointer_cast<ScalarType>(predicate->value_.scalar())->value;
    auto datum_val = predicate->value_;

    auto enum_comp = predicate->comparator_;
    if (enum_comp == arrow::compute::CompareOperator::NOT_EQUAL) {
      auto num_rows = block->get_num_rows();
      auto col_data = block->get_column_by_name(predicate->col_ref_.col_name)
                          ->data()
                          ->GetValues<CType>(1);

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

      auto f = [](uint8_t val, uint8_t diff) -> bool { return val <= diff; };
      for (uint32_t i = 0; i < num_rows; ++i) {
        bytemap[i] = f(col_data[i] - lo, diff);
      }

      std::shared_ptr<arrow::BooleanArray> out_filter;
      utils::pack(num_rows, bytemap, &out_filter);
      return out_filter;

    } else {
      return comparator_switcher(enum_comp, [&](auto op) {
        return Filter<CType>(block, predicate->col_ref_, datum_val, val,
                             predicate->comparator_, op);
      });
    }
  };

  // Set `result` to return value through the filter_handler().
  arrow::Datum result;
  auto filter_handler = [&]<typename T>(T *ptr) {
    if constexpr (has_ctype_member<T>::value) {
      result = numeric_filter(ptr);
    } else {
      result = default_filter(ptr);
    }
  };

  type_switcher(data_type, filter_handler);

  return result;
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
  const T *col_data;

  if (block->IsMetadataCompatible()) {
    auto metadata_block =
        std::static_pointer_cast<MetadataAttachedBlock>(block);
    if (!metadata_block->SearchMetadata(col_ref.col_name, arrow_val,
                                        arrow_compare)) {
      const T zero_val = (T)0;
      for (uint32_t i = 0; i < num_rows; ++i) {
        bytemap[i] = zero_val;
      }
    } else {
      col_data =
          block->get_column_by_name(col_ref.col_name)->data()->GetValues<T>(1);
      for (uint32_t i = 0; i < num_rows; ++i) {
        bytemap[i] = comparator(col_data[i], value);
      }
    }
  } else {
    col_data =
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
