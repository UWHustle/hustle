//
// Created by Nicholas Corrado on 4/12/20.
//

#include "JoinGraph.h"

namespace hustle::operators {

    JoinGraph::JoinGraph(std::vector<std::shared_ptr<Table>> tables,
            std::vector<std::vector<JoinPredicate>> trees) {

        for (int i=0; i<tables.size(); i++) {
            tables_.push_back(tables[i]);
            adj_.push_back(trees[i]);
        }
    }

    void JoinGraph::insert(std::shared_ptr<Table> table,
            std::vector<JoinPredicate> predicates) {

        // If table is already in the graph
        auto it = std::find(tables_.begin(), tables_.end(), table);
        if (it != tables_.end()) {
            int i = it - tables_.end();
            for (auto &predicate : predicates) {
                adj_[i].push_back(predicate);
            }
        }
        // If table is not in the graph
        else {
            tables_.push_back(table);
            adj_.push_back(predicates);
        }
    }

    std::shared_ptr<Table> JoinGraph::get_table(int i) {
        return tables_[i];

    }
    std::vector<JoinPredicate>
    JoinGraph::get_predicates(const std::shared_ptr<Table>& table) {
        auto it = std::find(tables_.begin(), tables_.end(), table);
        return adj_[it - tables_.end()];
    }


}