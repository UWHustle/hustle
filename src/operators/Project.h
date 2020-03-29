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
    std::shared_ptr<Table> table;
    arrow::compute::Datum selection;
    std::vector<std::shared_ptr<arrow::Field>> fields;
};

class Projection : public Operator {
public:

    Projection(std::vector<ProjectionUnit> projection_units);
    std::shared_ptr<Table> Project(std::vector<ProjectionUnit> projection_units);

    std::shared_ptr<Table> run_operator
            (std::vector<std::shared_ptr<Table>> tables);

};


}
}

#endif //HUSTLE_PROJECT_H
