#ifndef HUSTLE_SRC_TYPES_TYPES_H_
#define HUSTLE_SRC_TYPES_TYPES_H_

#include <better-enums/enum.h>

namespace hustle {
namespace types {

BETTER_ENUM(ComparativeType, int, NE, EQ, GT, LE, LT, GE)

BETTER_ENUM(ArithmeticType, int, PLUS, MINUS, STAR, SLASH)

BETTER_ENUM(AggFuncType, int, AVG, COUNT, SUM)

BETTER_ENUM(ExprType, int, ColumnReference, IntLiteral, StrLiteral, Comparative,
            Disjunctive, Conjunctive, Arithmetic, AggFunc)

BETTER_ENUM(QueryOperatorType, int, TableReference, Select, Project, Join,
            Aggregate, OrderBy)

BETTER_ENUM(OrderByDirection, int, ASC, DESC)

// TODO(Lichengxi): to add more
BETTER_ENUM(PlanType, int, Query, Create)

}  // namespace types
}  // namespace hustle
#endif  // HUSTLE_SRC_TYPES_TYPES_H_
