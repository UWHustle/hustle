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

#ifndef HUSTLE_BASE_AGGREGATE_H
#define HUSTLE_BASE_AGGREGATE_H

#include "operator.h"

namespace hustle::operators {

class BaseAggregate: public Operator {
protected:

  explicit BaseAggregate(const size_t query_id)
  : BaseAggregate(query_id, std::make_shared<OperatorOptions>()){};

  explicit BaseAggregate(const size_t query_id,
                         std::shared_ptr<OperatorOptions> sharedPtr)
    : Operator(query_id, sharedPtr) {};

};

}; // namespace hustle::operators

#endif //HUSTLE_BASE_AGGREGATE_H
