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

#ifndef HUSTLE_JOINGRAPH_H
#define HUSTLE_JOINGRAPH_H

#include "operators/select/predicate.h"
#include "storage/base_table.h"

namespace hustle {
namespace operators {

class JoinGraph {
 public:
  /**
   * Construct a directed multigraph where tables are node and edges are join
   * predicates. Edges exit the left table node and enter the right table
   * node.
   *
   * "predicates" refers to all edges on the graph.
   * "predicate group" referes to all edges on a graph originating from the
   * same node, i.e. all edges that share the same left table.
   *
   * Only simple join predicates without connective operators are supported
   * right now.
   *
   * @param predicates A vector of predicate groups. The vector at
   * predicates[i] corresponds to the predicate group for the ith left table.
   */
  explicit JoinGraph(std::vector<std::vector<JoinPredicate>> predicates);

  /**
   * Construct an empty join graph.
   */
  JoinGraph();

  /**
   * Get the table at a particular index.
   *
   * @param index
   * @return a table.
   */
  std::shared_ptr<HustleTable> get_table(int index);

  /**
   * Get the predicate group of a particular table
   *
   * @param table
   * @return A vector of all predicates whose left table is table.
   */
  std::vector<JoinPredicate> get_predicates(
      const std::shared_ptr<HustleTable>& table);

  /**
   * Get the predicate group of the table at index i.
   *
   * @param i
   * @return A vector of all predicates whose left table is table.
   */
  std::vector<JoinPredicate> get_predicates(int i);

  /**
   * Return the cardinality of the graph
   *
   * @return The number of tables in the graph.
   */
  inline size_t num_tables() { return adj_.size(); }

  inline size_t num_predicates() {
      return num_predicates_;
  }

 private:
  // Vector of unique left tables
  std::vector<std::shared_ptr<HustleTable>> tables_;
  // The vector at adj_[i] corresponds to all join predicates for which
  // tables_[i] is the left table.
  std::vector<std::vector<JoinPredicate>> adj_;

  // num of predicates present in the join graph
  size_t num_predicates_ = 0;

  /**
   * Insert a predicate group into the graph. If a node with the same left
   * table already exists, these predicates will be appended to that node's
   * predicate group.
   *
   * @param predicates a vector of predicates that share the same left table.
   */
  void insert(std::vector<JoinPredicate> predicate_group);

  /**
   * Return the index of a table in the graph.
   *
   * @param table
   * @return The index of table.
   */
  int find_table(std::shared_ptr<HustleTable> table);
};

}  // namespace operators
}  // namespace hustle

#endif  // HUSTLE_JOINGRAPH_H
