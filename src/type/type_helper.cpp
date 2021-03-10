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

namespace details {
std::shared_ptr<arrow::DataType> TestFields(arrow::Type::type type_enum) {
  static std::shared_ptr<arrow::DataType> dataTypes[] = {
      arrow::null(),
      arrow::boolean(),
      arrow::uint8(),
      arrow::int8(),
      arrow::uint16(),
      arrow::int16(),
      arrow::uint32(),
      arrow::int32(),
      arrow::uint64(),
      arrow::int64(),
      arrow::float16(),
      arrow::float32(),
      arrow::float64(),
      arrow::utf8(),
      arrow::binary(),
      arrow::fixed_size_binary(32),
      arrow::date32(),
      arrow::date64(),
      arrow::timestamp(arrow::TimeUnit::MICRO),
      arrow::time32(arrow::TimeUnit::SECOND),
      arrow::time64(arrow::TimeUnit::MICRO),  // micro / nano
      arrow::month_interval(),
      arrow::day_time_interval(),
      arrow::decimal(9, 10),
      arrow::list(arrow::int8()),
      arrow::struct_({arrow::field("a", arrow::int8()),
                      arrow::field("b", arrow::int32())}),
      arrow::sparse_union({arrow::field("a", arrow::int8()),
                           arrow::field("b", arrow::int32())}),
      arrow::dense_union({arrow::field("a", arrow::int8()),
                          arrow::field("b", arrow::int32())}),
      arrow::dictionary(arrow::int32(), arrow::utf8()),
      arrow::map(arrow::binary(), arrow::int64()),
      nullptr,
      arrow::fixed_size_list(arrow::int64(), 10),
      arrow::duration(arrow::TimeUnit::MICRO),
      arrow::large_utf8(),
      arrow::large_binary(),
      arrow::large_list(arrow::int8()),
      nullptr};
  int item_index = int(type_enum);
  return dataTypes[item_index];
}
}  // namespace details

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

}  // namespace hustle
