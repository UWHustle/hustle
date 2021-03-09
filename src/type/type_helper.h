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

#ifndef HUSTLE_TYPE_HELPER_H
#define HUSTLE_TYPE_HELPER_H

#include <arrow/api.h>

#include <iostream>

namespace hustle {
// Placeholder case statement. Simply throws error at runtime.
#define HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_)                       \
  {                                                                         \
    std::cerr                                                               \
        << "Used the default arrow type case statement placeholder macro. " \
           "Please Define a macro "                                         \
           "HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_) in "              \
           "your run time scope, and undef it when finished."               \
        << std::endl;                                                       \
    exit(0);                                                                \
  }

// Default macro that handles the MAX_ID case.
// Simply throw an error and abort.
#define HUSTLE_ARROW_MAX_ID_CASE_STMT()                                  \
  case Type::MAX_ID: {                                                   \
    std::cerr << "Encountered MAX_ID in switch statement." << std::endl; \
    exit(1);                                                             \
  }

// Individual switch statement that transforms
// an arrow enum type to an arrow DataType type.
#define _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow_enum_type_, data_type_) \
  case arrow_enum_type_: {                                           \
    HUSTLE_ARROW_TYPE_CASE_STMT(data_type_);                         \
  };

// The Big switch statement that make arrow type transform to the DataType type
#define HUSTLE_SWITCH_ARROW_TYPE(arrow_enum_type_)                          \
  switch (arrow_enum_type_) {                                               \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::NA, arrow::NullType);       \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::BOOL, arrow::BooleanType);  \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT8, arrow::Int8Type);     \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT16, arrow::Int16Type);   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT32, arrow::Int32Type);   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT64, arrow::Int64Type);   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT8, arrow::UInt8Type);   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT16, arrow::UInt16Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT32, arrow::UInt32Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT64, arrow::UInt64Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::HALF_FLOAT,                 \
                                   arrow::HalfFloatType);                   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FLOAT, arrow::FloatType);   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DOUBLE, arrow::DoubleType); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::STRING, arrow::StringType); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::BINARY, arrow::BinaryType); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_STRING,               \
                                   arrow::LargeStringType);                 \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_BINARY,               \
                                   arrow::LargeBinaryType);                 \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FIXED_SIZE_BINARY,          \
                                   arrow::FixedSizeBinaryType);             \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DATE32, arrow::Date32Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DATE64, arrow::Date64Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIME32, arrow::Time32Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIME64, arrow::Time64Type); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIMESTAMP,                  \
                                   arrow::TimestampType);                   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INTERVAL_DAY_TIME,          \
                                   arrow::DayTimeIntervalType);             \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INTERVAL_MONTHS,            \
                                   arrow::MonthIntervalType);               \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DURATION,                   \
                                   arrow::DurationType);                    \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DECIMAL,                    \
                                   arrow::Decimal128Type);                  \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::STRUCT, arrow::StructType); \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LIST, arrow::ListType);     \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_LIST,                 \
                                   arrow::LargeListType);                   \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FIXED_SIZE_LIST,            \
                                   arrow::FixedSizeListType);               \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::MAP, arrow::MapType);       \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DENSE_UNION,                \
                                   arrow::DenseUnionType);                  \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::SPARSE_UNION,               \
                                   arrow::SparseUnionType);                 \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DICTIONARY,                 \
                                   arrow::DictionaryType);                  \
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::EXTENSION,                  \
                                   arrow::ExtensionType);                   \
    HUSTLE_ARROW_MAX_ID_CASE_STMT();                                        \
  };  // namespace hustle

}  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H
