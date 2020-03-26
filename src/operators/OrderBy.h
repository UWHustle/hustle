//
// Created by Nicholas Corrado on 3/26/20.
//

#ifndef HUSTLE_ORDERBY_H
#define HUSTLE_ORDERBY_H

#include <arrow/api.h>
#include <table/table.h>
#include "Operator.h"

namespace hustle {
namespace operators {

class OrderBy : public Operator {
public:

    OrderBy(const std::shared_ptr<Table>& table, std::shared_ptr<arrow::Field>
            order_by_field);

    std::shared_ptr<Table> run_operator
            (std::vector<std::shared_ptr<Table>> tables) override;

private:
    std::shared_ptr<arrow::Field> order_by_field_;
};

}
}

#endif //HUSTLE_ORDERBY_H
