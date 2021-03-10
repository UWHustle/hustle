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

#include "type_helper.h"

std::shared_ptr<arrow::ArrayBuilder> getBuilder(
    const std::shared_ptr<arrow::DataType> &dataType) {
#undef HUSTLE_ARROW_TYPE_CASE_STMT
#define HUSTLE_ARROW_TYPE_CASE_STMT(T)          \
  {                                             \
    auto factory = BuilderFactory<T>(dataType); \
    auto result = factory.GetBuilder();         \
    return result.ValueOrDie();                 \
  }
  auto enum_type = dataType->id();
  HUSTLE_SWITCH_ARROW_TYPE(enum_type);
#undef HUSTLE_ARROW_TYPE_CASE_STMT
  return nullptr;
};
