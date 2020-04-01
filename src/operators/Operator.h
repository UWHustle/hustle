#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

namespace hustle {
namespace operators {

enum FilterOperator {
    AND,
    OR,
    NONE
};

//TODO(nicholas): Needs a more descriptive name
struct SelectionReference {
    std::shared_ptr<Table> table;
    arrow::compute::Datum selection;
};

struct ColumnReference {
    std::shared_ptr<Table> table;
    std::string col_name;
};

struct GroupReference {
    std::shared_ptr<Table> table;
    std::vector<std::string> col_names;
};

class Operator {
public:

    virtual std::shared_ptr<Table> run_operator
    (std::vector<std::shared_ptr<Table>> tables) = 0;
};
} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
