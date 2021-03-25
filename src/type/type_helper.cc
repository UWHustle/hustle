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
namespace hustle {

std::unique_ptr<arrow::ArrayBuilder> getBuilder(
    const std::shared_ptr<arrow::DataType> &dataType) {
  std::unique_ptr<arrow::ArrayBuilder> out;
  auto status =
      arrow::MakeBuilder(arrow::default_memory_pool(), dataType, &out);
  if (!status.ok()) {
    throw std::runtime_error(std::string("Make builder failed: ") +
                             dataType->ToString());
  }
  return out;
};

std::unique_ptr<arrow::ArrayBuilder> getBuilder(
    arrow::MemoryPool* memory_pool,
    const std::shared_ptr<arrow::DataType> &dataType) {
  std::unique_ptr<arrow::ArrayBuilder> out;
  auto status =
      arrow::MakeBuilder(memory_pool, dataType, &out);
  if (!status.ok()) {
    throw std::runtime_error(std::string("Make builder failed: ") +
                             dataType->ToString());
  }
  return out;
};

}  // namespace hustle
