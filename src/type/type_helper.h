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

//
// Helper macros that generates all switch statements to handle Arrow types.
//
// Basic usage:
//
//  // 1. Disable compiler warning
//  #undef HUSTLE_ARROW_TYPE_CASE_STMT
//
//  // 2. Define the case body. This will be called for each arrow type.
//  //    arrow_data_type_: a subclass of arrow::DataType
//  #define HUSTLE_ARROW_TYPE_CASE_STMT \
//        { std::cout << arrow_data_type_::name() << std::endl; }
//
//  // 3. Call the switch statement
//  auto enum_type = arrow::Type::INT8; // Some dynmaic enum type...
//  HUSTLE_SWITCH_ARROW_TYPE(enum_type);
//
//  // 4. Reset the macro to prevent accidental reuse.
//  #undef HUSTLE_ARROW_TYPE_CASE_STMT
//
//
// Example usage:
//  - src/operators/expression.h: ExecuteBlockHandler.

// Placeholder case statement. Overwrite your macro definition when you use it.
// The default statement simply throws error at runtime.
#define HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_)                       \
  {                                                                         \
    std::cerr                                                               \
        << "Used the default arrow type case statement placeholder macro. " \
           "Please define the macro "                                       \
           "HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_) in "              \
           "your scope (and undef it at the end of the scope). "            \
           "See type_helper.h for usage."                                   \
        << std::endl;                                                       \
    exit(0);                                                                \
  }

// Default macro that handles the MAX_ID case.
// Simply throw an error and abort.
#define HUSTLE_ARROW_MAX_ID_CASE_STMT()                                  \
  case arrow::Type::MAX_ID: {                                            \
    std::cerr << "Encountered MAX_ID in switch statement." << std::endl; \
    exit(1);                                                             \
  }

// Individual switch statement that transforms
// an arrow enum type to an arrow DataType type.
#define HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow_enum_type_, data_type_) \
  case arrow_enum_type_: {                                          \
    HUSTLE_ARROW_TYPE_CASE_STMT(data_type_);                        \
    break;                                                          \
  };

// The Big switch statement that make arrow type transform to the DataType type
#define HUSTLE_SWITCH_ARROW_TYPE(arrow_enum_type_)                             \
  switch (arrow_enum_type_) {                                                  \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::NA, arrow::NullType);           \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::BOOL, arrow::BooleanType);      \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT8, arrow::Int8Type);         \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT16, arrow::Int16Type);       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT32, arrow::Int32Type);       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INT64, arrow::Int64Type);       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT8, arrow::UInt8Type);       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT16, arrow::UInt16Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT32, arrow::UInt32Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::UINT64, arrow::UInt64Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::HALF_FLOAT,                     \
                                  arrow::HalfFloatType);                       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FLOAT, arrow::FloatType);       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DOUBLE, arrow::DoubleType);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::STRING, arrow::StringType);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::BINARY, arrow::BinaryType);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_STRING,                   \
                                  arrow::LargeStringType);                     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_BINARY,                   \
                                  arrow::LargeBinaryType);                     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FIXED_SIZE_BINARY,              \
                                  arrow::FixedSizeBinaryType);                 \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DATE32, arrow::Date32Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DATE64, arrow::Date64Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIME32, arrow::Time32Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIME64, arrow::Time64Type);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::TIMESTAMP,                      \
                                  arrow::TimestampType);                       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INTERVAL_DAY_TIME,              \
                                  arrow::DayTimeIntervalType);                 \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::INTERVAL_MONTHS,                \
                                  arrow::MonthIntervalType);                   \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DURATION, arrow::DurationType); \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DECIMAL,                        \
                                  arrow::Decimal128Type);                      \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::STRUCT, arrow::StructType);     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LIST, arrow::ListType);         \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::LARGE_LIST,                     \
                                  arrow::LargeListType);                       \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::FIXED_SIZE_LIST,                \
                                  arrow::FixedSizeListType);                   \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::MAP, arrow::MapType);           \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DENSE_UNION,                    \
                                  arrow::DenseUnionType);                      \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::SPARSE_UNION,                   \
                                  arrow::SparseUnionType);                     \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DICTIONARY,                     \
                                  arrow::DictionaryType);                      \
    HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::EXTENSION,                      \
                                  arrow::ExtensionType);                       \
    HUSTLE_ARROW_MAX_ID_CASE_STMT();                                           \
  };

//
// has_builder_type
//
// Tells if a DataType has a builder
// and some properties about the builder.
//

template <typename, typename = void>
struct has_builder_type : std::false_type {
  using BuilderType = void;
  using is_defalut_constructable = std::false_type;
  static constexpr bool is_defalut_constructable_v = false;
};

template <typename DataType>
struct has_builder_type<
    DataType, std::void_t<typename arrow::TypeTraits<DataType>::BuilderType>>
    : std::true_type {
  using BuilderType = typename arrow::TypeTraits<DataType>::BuilderType;
  using is_defalut_constructable = std::is_default_constructible<BuilderType>;
  static constexpr bool is_defalut_constructable_v =
      is_defalut_constructable::value;
};

template <typename T, typename R = void>
using enable_if_builder_default_constructable =
    std::enable_if_t<has_builder_type<T>::is_defalut_constructable_v, R>;

template <typename T, typename R = void>
using enable_if_builder_non_default_constructable =
    std::enable_if_t<has_builder_type<T>::value &&
                         !has_builder_type<T>::is_defalut_constructable_v,
                     R>;

template <typename T, typename R = void>
using enable_if_no_builder = std::enable_if_t<!has_builder_type<T>::value, R>;

template <class, class = void>
struct has_ctype_member : std::false_type {};

template <class T>
struct has_ctype_member<T, std::void_t<typename arrow::TypeTraits<T>::CType>>
    : std::true_type {};

};  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H