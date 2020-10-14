//
// Created by Ginda on 10/14/20.
//

#ifndef HUSTLE_AGGREGATE_CONST_H
#define HUSTLE_AGGREGATE_CONST_H

#include "operators/operator.h"

namespace hustle::operators {


// Types of aggregates we can perform. COUNT is currently not supported.
enum AggregateKernel { SUM, COUNT, MEAN };

/**
 * A reference structure containing all the information needed to perform an
 * aggregate over a column.
 *
 * @param kernel The type of aggregate we want to compute
 * @param agg_name Name of the new aggregate column
 * @param col_ref A reference to the column over which we want to compute the
 * aggregate
 */
struct AggregateReference {
  AggregateKernel kernel;
  std::string agg_name;
  ColumnReference col_ref;
};

}

#endif //HUSTLE_AGGREGATE_CONST_H
