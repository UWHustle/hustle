#ifndef HUSTLE_RESOLVER_H
#define HUSTLE_RESOLVER_H

#include <parser/ParseTree.h>
#include <resolver/Plan.h>

using hustle::parser::ParseTree;

namespace hustle {
namespace resolver {

class Resolver {
 public:
  std::shared_ptr<hustle::resolver::Plan> resolve(const std::shared_ptr<ParseTree>& parse_tree) {
    return nullptr;
  }
};

}
}

#endif //HUSTLE_RESOLVER_H
