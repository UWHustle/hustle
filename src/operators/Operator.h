//
// Created by Nicholas Corrado on 4/26/20.
//

#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <cstdlib>
#include "OperatorResult.h"

namespace hustle::operators {

    class Operator {
        virtual std::shared_ptr<OperatorResult> run() = 0;
    public:

    };
};




#endif //HUSTLE_OPERATOR_H
