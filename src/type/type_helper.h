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
#include <arrow/compute/api.h>

#include <iostream>

namespace hustle {

// TODO: Put requirement constraints that Functor must only have one argument
//  that accepts a (const T * ptr_) and will never use it.
//  This is the current constraint that lambda templates can't be initialized
//  with non-type parameter.

//#define TYPE_ERROR(fn, lineno, mesg)

//
// Type Traits
//

template <typename DataType>
using ArrowGetBuilderType = typename arrow::TypeTraits<DataType>::BuilderType;

template <typename DataType>
using ArrowGetArrayType = typename arrow::TypeTraits<DataType>::ArrayType;

template <typename DataType>
using ArrowGetScalarType = typename arrow::TypeTraits<DataType>::ScalarType;

// template <typename DataType>
// using ArrowGetCType = typename arrow::TypeTraits<DataType>::CType;

template <typename DataType>
using ArrowGetCType = typename DataType::c_type;

template <typename, typename = void>
struct has_array_type : std::false_type {
  using ArrayType = void;
};

template <typename DataType>
struct has_array_type<
    DataType, std::void_t<typename arrow::TypeTraits<DataType>::ArrayType>>
    : std::true_type {
  using BuilderType = ArrowGetBuilderType<DataType>;
};

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
  using BuilderType = ArrowGetBuilderType<DataType>;
  using is_defalut_constructable = std::is_default_constructible<BuilderType>;
  static constexpr bool is_defalut_constructable_v =
      is_defalut_constructable::value;
};

// TODO: CType concept may be different. Some types have ctype nested in the
//      body but not in the trait, and some (primitives) will expose that.
template <class, class = void>
struct has_ctype_member : std::false_type {};

template <class T>
struct has_ctype_member<T, std::void_t<typename arrow::TypeTraits<T>::CType>>
    : std::true_type {};

template <typename T, typename... Ts>
using isOneOf = std::disjunction<std::is_same<T, Ts>...>;

template <typename T, typename... Ts>
using isNotOneOf = std::negation<isOneOf<T, Ts...>>;

template <typename DataType>
using is_string_type =
    isOneOf<DataType, arrow::StringType, arrow::LargeStringType>;

template <typename DataType>
using is_binary_type =
    isOneOf<DataType, arrow::BinaryType, arrow::LargeBinaryType>;

// Create Array Builder
//    Use CreateBuilder as the central function.
//

namespace details {

// ***
// *** DO NOT USE THIS MACRO UNLESS YOU ARE CERTAIN TO DO THAT! ***
// ***
// ***  See the following helpers as the first choice:
// ***     - type_switcher(): Generic switcher with type control.
// ***     - get_builder(): Get an arrow builder with type control.
//

// Helper macros that generates all switch statements to handle Arrow types.
//
// Basic usage:
//
//  // 1. Disable compiler warning
//  #undef _HUSTLE_ARROW_TYPE_CASE_STMT
//
//  // 2. Define the case body. This will be called for each arrow type.
//  //    arrow_data_type_: a subclass of arrow::DataType
//  #define _HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_) \
//        { std::cout << arrow_data_type_::name() << std::endl; }
//
//  // 3. Call the switch statement
//  auto enum_type = arrow::Type::INT8; // Some dynmaic enum type...
//  _HUSTLE_SWITCH_ARROW_TYPE(enum_type);
//
//  // 4. Reset the macro to prevent accidental reuse.
//  #undef _HUSTLE_ARROW_TYPE_CASE_STMT
//
//
// Example usage:
//  - src/operators/expression.h: ExecuteBlockHandler.

// Placeholder case statement. Overwrite your macro definition when you use it.
// The default statement simply throws error at runtime.
#define _HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_)                      \
  {                                                                         \
    std::cerr                                                               \
        << "Used the default arrow type case statement placeholder macro. " \
           "Please define the macro "                                       \
           "_HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_) in "             \
           "your scope (and undef it at the end of the scope). "            \
           "See type_helper.h for usage."                                   \
        << std::endl;                                                       \
    exit(0);                                                                \
  }

// Default macro that handles the MAX_ID case.
// Simply throw an error and abort.
#define _HUSTLE_ARROW_MAX_ID_CASE_STMT()                                 \
  case arrow::Type::MAX_ID: {                                            \
    std::cerr << "Encountered MAX_ID in switch statement." << std::endl; \
    exit(1);                                                             \
  }

// Individual switch statement that transforms
// an arrow enum type to an arrow DataType type.
#define _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow_enum_type_, data_type_) \
  case arrow_enum_type_: {                                           \
    _HUSTLE_ARROW_TYPE_CASE_STMT(data_type_);                        \
    break;                                                           \
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
    _HUSTLE_ARROW_TYPE_SWITCH_CASE(arrow::Type::DECIMAL256,                 \
                                   arrow::Decimal256Type);                  \
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
    _HUSTLE_ARROW_MAX_ID_CASE_STMT();                                       \
  };

enum BuildCategory {
  // Type classification
  // [1] Independent. Type does not depend on the field at all.
  //    null bool int8 int16 int32 int64 uint8 uint16 uint32 uint64
  //    halffloat float double
  //    date32 date64
  //    day_time_interval month_interval
  //    utf8 binary large_utf8 large_binary
  // [2] Required Identity Type Constructable.
  // Must/May be good to construct from its own type with specification.
  //    time32 time64 timestamp duration
  //    decimal fixed_size_binary
  // [3] List-like type. Provide a data type and construct builder.
  // [4] Struct type. Need to construct the builder for each fields.
  // [5] Map. Need to construct the index and value builder.
  // [6] DenseUnion, SparseUnion.
  // [7] dictionary: Need to specify the exact typing, and establish builders.
  //      DictionaryBuilder<T>
  // [8] Extension: No support. Extension type does not have a builder.
  independent = 0,
  required_identity = 1,
  list_like_type = 2,
  struct_type = 3,
  map_type = 4,
  union_type = 5,
  dict_type = 6,
  extension_type = 7
};

template <typename T>
static constexpr BuildCategory builder_category() {
  // TODO: Replace the magic numbers to some enum
  // [1] Independent Type
  constexpr bool class_1 =
      std::disjunction_v<arrow::is_number_type<T>, arrow::is_boolean_type<T>,
                         arrow::is_null_type<T>, arrow::is_interval_type<T>,
                         is_binary_type<T>, is_string_type<T>,
                         arrow::is_date_type<T>>;

  // [2] Required identity type construction
  constexpr bool class_2 =
      std::disjunction_v<arrow::is_timestamp_type<T>,
                         arrow::is_duration_type<T>, arrow::is_decimal_type<T>,
                         arrow::is_time_type<T>,
                         std::is_same<T, arrow::FixedSizeBinaryType>>;

  if constexpr (class_1) {
    return BuildCategory::independent;
  }
  if constexpr (class_2) {
    return BuildCategory::required_identity;
  }
  if constexpr (std::is_same_v<T, arrow::MapType>) {
    return BuildCategory::map_type;
  }
  if constexpr (arrow::is_list_like_type<T>::value) {
    return BuildCategory::list_like_type;
  }
  if constexpr (std::is_same_v<T, arrow::StructType>) {
    return BuildCategory::struct_type;
  }
  if constexpr (arrow::is_union_type<T>::value) {
    return BuildCategory::union_type;
  }
  if constexpr (std::is_same_v<T, arrow::DictionaryType>) {
    return BuildCategory::dict_type;
  }
  // Extension type not support for builder
  return BuildCategory::extension_type;
}

}  // namespace details

// Switcher: arrow enum types -> arrow DataType child classes.
// Must put the definition in header until g++ resolve the error.
// See: https://bit.ly/3bMbPG2
template <typename ArrowDataTypeSwitchFunctor>
void type_switcher(const std::shared_ptr<arrow::DataType> &dataType,
                   const ArrowDataTypeSwitchFunctor &func) {
#undef _HUSTLE_ARROW_TYPE_CASE_STMT
#define _HUSTLE_ARROW_TYPE_CASE_STMT(T) \
  { func((T *)nullptr); }
  HUSTLE_SWITCH_ARROW_TYPE(dataType->id());
#undef _HUSTLE_ARROW_TYPE_CASE_STMT
};

// Switcher: arrow comparator enum -> std comparator
template <typename ArrowComputeOperatorSwitchFunctor>
auto comparator_switcher(arrow::compute::CompareOperator c,
                         const ArrowComputeOperatorSwitchFunctor &func)
    // Take the return type of the functor.
    -> decltype(func(std::equal_to())) {
  switch (c) {
    case arrow::compute::EQUAL:
      return func(std::equal_to());
    case arrow::compute::NOT_EQUAL:
      return func(std::not_equal_to());
    case arrow::compute::GREATER:
      return func(std::greater());
    case arrow::compute::GREATER_EQUAL:
      return func(std::greater_equal());
    case arrow::compute::LESS:
      return func(std::less());
    case arrow::compute::LESS_EQUAL:
      return func(std::less_equal());
  };
};

std::unique_ptr<arrow::ArrayBuilder> getBuilder(
    const std::shared_ptr<arrow::DataType> &dataType);

};  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H
