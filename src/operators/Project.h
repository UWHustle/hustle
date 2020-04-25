//
// Created by Nicholas Corrado on 3/24/20.
//

#ifndef HUSTLE_PROJECT_H
#define HUSTLE_PROJECT_H
#include <arrow/api.h>
#include <table/table.h>
#include <arrow/compute/api.h>
#include "Operator.h"



namespace hustle {
namespace operators {

struct ProjectionUnit {
    LazyTable ref;
    std::vector<std::shared_ptr<arrow::Field>> fields;
};

class Projection : public Operator {
public:

    Projection(std::shared_ptr<OperatorResult> prev_result,
            std::vector<ColumnReference> col_refs);
    std::shared_ptr<Table> project();
    std::shared_ptr<OperatorResult> run() override;

private:

    std::shared_ptr<OperatorResult> prev_result_;
    std::vector<ColumnReference> col_refs_;
};


}
}

#endif //HUSTLE_PROJECT_H
