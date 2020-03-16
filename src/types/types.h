#ifndef HUSTLE_SRC_RESOLVER_TYPES_H_
#define HUSTLE_SRC_RESOLVER_TYPES_H_

#include <better-enums/enum.h>

namespace hustle {
namespace types {

BETTER_ENUM (
    ComparativeType,
    int,
    NE,
    EQ,
    GT,
    LE,
    LT,
    GE
)

BETTER_ENUM (
    ArithmeticType,
    int,
    PLUS,
    MINUS,
    STAR,
    SLASH
)


BETTER_ENUM (
    ExprType,
    int,
    ColumnReference,
    IntLiteral,
    StrLiteral,
    Comparative,
    Disjunctive,
    Conjunctive,
    Arithmetic,
    AggFunc
)

BETTER_ENUM (
    QueryOperatorType,
    int,
    TableReference,
    Select,
    Project,
    Join,
    GroupBy,
    OrderBy
)

BETTER_ENUM (
    PlanType,
    int,
    Query,
    Create
)

}
}
#endif //HUSTLE_SRC_RESOLVER_TYPES_H_
