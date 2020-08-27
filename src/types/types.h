// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
