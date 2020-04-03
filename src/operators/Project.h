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
    SelectionReference ref;
    std::vector<std::shared_ptr<arrow::Field>> fields;
};

class Projection : public Operator {
public:

    Projection(std::vector<ProjectionUnit> projection_units);
    std::shared_ptr<Table> project();

private:
    std::vector<ProjectionUnit> projection_units_;
};


}
}

#endif //HUSTLE_PROJECT_H
