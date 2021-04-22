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

#ifndef HUSTLE_PREDICATE_STRUCTS_H
#define HUSTLE_PREDICATE_STRUCTS_H

#include "operators/utils/predicate_structs.h"

namespace hustle::operators {
enum CompareOperator {
  EQUAL,
  NOT_EQUAL,
  LESS,
  LESS_EQUAL,
  GREATER,
  GREATER_EQUAL,
  BETWEEN
};

enum FilterOperator {
  AND,
  OR,
  NONE,
};

struct Predicate {
  ColumnReference col_ref_;
  arrow::compute::CompareOperator comparator_;
  arrow::Datum value_;
  arrow::Datum value2_;
};

struct JoinPredicate {
  ColumnReference left_col_;
  arrow::compute::CompareOperator comparator_;
  ColumnReference right_col_;

  bool operator==(const JoinPredicate& join_predicate) const {
    return (left_col_ == join_predicate.left_col_ &&
            right_col_ == join_predicate.right_col_) &&
           comparator_ == join_predicate.comparator_;
  }

  bool operator!=(const JoinPredicate& join_predicate) const {
    return (left_col_ != join_predicate.left_col_ ||
            right_col_ != join_predicate.right_col_) ||
           comparator_ != join_predicate.comparator_;
  }
};

struct OrderByReference {
  ColumnReference col_ref_;
  bool is_desc;
};

using JoinPredicatePtr = std::shared_ptr<JoinPredicate>;
}  // namespace hustle::operators

#endif  // HUSTLE_PREDICATE_STRUCTS_H
