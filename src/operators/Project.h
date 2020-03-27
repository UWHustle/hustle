//
// Created by Nicholas Corrado on 3/24/20.
//

#ifndef HUSTLE_PROJECT_H
#define HUSTLE_PROJECT_H
#include <arrow/api.h>
#include <table/table.h>
#include "Operator.h"



namespace hustle {
namespace operators {

class Project : public Operator {
public:

    Project(std::shared_ptr<arrow::Schema> schema);


};


}
}

#endif //HUSTLE_PROJECT_H
