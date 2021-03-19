
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

#ifndef HUSTLE_SELECT_BUILD_HASH_H
#define HUSTLE_SELECT_BUILD_HASH_H

#include <arrow/compute/api.h>

#include <string>

#include "operators/operator.h"
#include "operators/select/predicate.h"
#include "operators/select/select.h"
#include "operators/utils/operator_result.h"
#include "storage/block.h"
#include "storage/table.h"

namespace hustle::operators {

/**
 * The Select operator updates the filter of a LazyTable so that it filters out
 * all tuples that do not satisfy the selection predicate.
 *
 * Predicates are inputted as a predicate tree. All internal nodes of the tree
 * are connective operators (AND, OR), while leaf nodes are simple predicates,
 * e.g. column = 7.
 */
class SelectBuildHash : public Select {
 public:
  /**
   * Construct a Select operator.
   *
   * @param prev_result OperatorResult from an upstream operator
   * @param tree predicate tree
   */
  SelectBuildHash(const std::size_t query_id, DBTable::TablePtr table,
                  OperatorResult::OpResultPtr prev_result,
                  OperatorResult::OpResultPtr output_result,
                  std::shared_ptr<PredicateTree> tree,
                  ColumnReference join_column);

  SelectBuildHash(const std::size_t query_id, DBTable::TablePtr table,
                  OperatorResult::OpResultPtr prev_result,
                  OperatorResult::OpResultPtr output_result,
                  std::shared_ptr<PredicateTree> tree,
                  ColumnReference join_column,
                  std::shared_ptr<OperatorOptions> options);

  /**
   * Perform the selection specified by the predicate tree passed into the
   * constructor.
   *
   * @return a new OperatorResult with an updated filter.
   */
  void execute(Task *ctx) override;

  void Clear() override {}

 private:
  ColumnReference join_column_;

  std::vector<uint64_t> chunk_row_offsets_;
  std::shared_ptr<phmap::flat_hash_map<int64_t, std::vector<RecordID>>> hash_table_;

  void ExecuteBlock(int block_index);
};

}  // namespace hustle::operators

#endif  // HUSTLE_SELECT_BUILD_HASH_H
