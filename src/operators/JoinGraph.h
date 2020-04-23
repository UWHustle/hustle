//
// Created by Nicholas Corrado on 4/12/20.
//

#ifndef HUSTLE_JOINGRAPH_H
#define HUSTLE_JOINGRAPH_H

#include "table/table.h"
#include "Predicate.h"

namespace hustle{
namespace operators {

class JoinGraph {

public:

    JoinGraph(
              std::vector<std::vector<JoinPredicate>> trees);
    std::shared_ptr<Table> get_table(int index);
    std::vector<JoinPredicate>
        get_predicates(const std::shared_ptr<Table>& table);
    std::vector<JoinPredicate> get_predicates(int i);
private:

// TODO(nicholas): We could use table id's but then what if the id was,
    //  for example, 37? Would we want to put it at index 37 or the next
    //  available index?
    std::vector<std::shared_ptr<Table>> tables_;
    std::vector<std::vector<JoinPredicate>> adj_;
    void insert(std::shared_ptr<Table> table,
                           std::vector<JoinPredicate> predicates);



};


}
}




#endif //HUSTLE_JOINGRAPH_H
