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

#include "join_graph.h"

namespace hustle::operators {

JoinGraph::JoinGraph() = default;

JoinGraph::JoinGraph(std::vector<std::vector<JoinPredicate>> join_predicates) {
  for (auto& join_predicate : join_predicates) {
    adj_.push_back(join_predicate);
    tables_.push_back(join_predicate[0].left_col_ref_.table);
  }
}

void JoinGraph::insert(std::vector<JoinPredicate> predicate_group) {
  auto table = predicate_group[0].left_col_ref_.table;
  // If table is already in the graph
  auto it = std::find(tables_.begin(), tables_.end(), table);
  if (it != tables_.end()) {
    int i = it - tables_.end();
    for (auto& predicate : predicate_group) {
      adj_[i].push_back(predicate);
    }
  }
  // If table is not in the graph
  else {
    tables_.push_back(table);
    adj_.push_back(predicate_group);
  }
}

std::shared_ptr<Table> JoinGraph::get_table(int i) { return tables_[i]; }
std::vector<JoinPredicate> JoinGraph::get_predicates(
    const std::shared_ptr<Table>& table) {
  auto it = std::find(tables_.begin(), tables_.end(), table);
  return adj_[it - tables_.end()];
}

std::vector<JoinPredicate> JoinGraph::get_predicates(int i) { return adj_[i]; }

int JoinGraph::get_num_tables() { return adj_.size(); }

int JoinGraph::find_table(std::shared_ptr<Table> table) {
  auto it = std::find(tables_.begin(), tables_.end(), table);
  return it != tables_.end();
}

}  // namespace hustle::operators