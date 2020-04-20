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
    JoinGraph(std::vector<std::shared_ptr<Table>> tables,
              std::vector<std::shared_ptr<PredicateTree>> trees);
private:

// TODO(nicholas): We could use table id's but then what if the id was,
    //  for example, 37? Would we want to put it at index 37 or the next
    //  available index?
    std::unordered_map<std::shared_ptr<Table>, int> table_to_int;
    std::vector<std::shared_ptr<PredicateTree>> adj;

    std::shared_ptr<PredicateTree>
        get_predicate_tree(const std::shared_ptr<Table>& table);

};


}
}




#endif //HUSTLE_JOINGRAPH_H
