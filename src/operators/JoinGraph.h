//
// Created by Nicholas Corrado on 4/12/20.
//

#ifndef HUSTLE_JOINGRAPH_H
#define HUSTLE_JOINGRAPH_H

#include "Predicate.h"
#include "table/table.h"

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
  std::shared_ptr<Table> get_table(int index);

  /**
   * Get the predicate group of a particular table
   *
   * @param table
   * @return A vector of all predicates whose left table is table.
   */
  std::vector<JoinPredicate> get_predicates(
      const std::shared_ptr<Table>& table);

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
  int get_num_tables();

 private:
  // Vector of unique left tables
  std::vector<std::shared_ptr<Table>> tables_;
  // The vector at adj_[i] corresponds to all join predicates for which
  // tables_[i] is the left table.
  std::vector<std::vector<JoinPredicate>> adj_;

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
  int find_table(std::shared_ptr<Table> table);
};

}  // namespace operators
}  // namespace hustle

#endif  // HUSTLE_JOINGRAPH_H
