#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>
#include <table/table.h>

namespace hustle {
namespace operators {

enum FilterOperator {
    AND,
    OR,
    NONE
};

class Operator {
public:

    virtual std::shared_ptr<Table> run_operator
    (std::vector<std::shared_ptr<Table>> tables) = 0;
};
} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
