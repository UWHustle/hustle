//
// Created by Nicholas Corrado on 4/12/20.
//

#include "JoinGraph.h"

namespace hustle::operators {

    JoinGraph::JoinGraph(std::vector<std::shared_ptr<Table>> tables,
            std::vector<std::shared_ptr<PredicateTree>> trees) {

        for (int i=0; i<tables.size(); i++) {
            table_to_int[tables[i]] = i;
            adj[i] = trees[i];
        }
    }

    std::shared_ptr<PredicateTree>
    JoinGraph::get_predicate_tree(const std::shared_ptr<Table>& table) {

        return adj[table_to_int[table]];
    }


}