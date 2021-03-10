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
// Type Traits
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

// TODO: CType concept may be different. Some types have ctype nested in the
//      body but not in the trait, and some (primitives) will expose that.
template <class, class = void>
struct has_ctype_member : std::false_type {};

template <class T>
struct has_ctype_member<T, std::void_t<typename arrow::TypeTraits<T>::CType>>
    : std::true_type {};

// TODO: Make these two builder factory a class?
// Create Array Builder
//    Use CreateBuilder as the central function.
//
//template <typename T>
//std::shared_ptr<arrow::ArrayBuilder> _CreateDefaultBuilder(
//    arrow::MemoryPool*) {
//  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
//  return std::make_shared<BuilderType>();
//}
//
//template <typename T>
//std::shared_ptr<arrow::ArrayBuilder> _CreateBuilder_TypeDefault(
//    arrow::MemoryPool* mem_pool = arrow::default_memory_pool()) {
//  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
//  auto datatype = std::make_shared<T>();
//  return std::make_shared<BuilderType>(datatype, mem_pool);
//}
//
//template <typename T>
//std::shared_ptr<arrow::ArrayBuilder> CreateBuilder(
//    arrow::MemoryPool* mem_pool = arrow::default_memory_pool()) {
//  // [1] Builder is default constructable.
//  if constexpr (has_builder_type<T>::value &&
//                has_builder_type<T>::is_defalut_constructable_v) {
//    return _CreateDefaultBuilder<T>(mem_pool);
//  }
//  // [2] DataType is default constructable.
//  if constexpr (std::is_default_constructible_v<T>) {
//    return _CreateBuilder_TypeDefault<T>(mem_pool);
//  }
//  // TODO: I am hesitate to make this a compile time error.
//  // [3] We can do nothing if it only has a default constructor.
//  throw std::runtime_error("Builder cannot default construct for type: " +
//                           T::type_name());
//}

//
// Create Array From Field
//
// Use CreateArrayBuilderFromField as the central function.
// Type classification
// [1] Independent. Type does not depend on the field at all.
//    null bool int8 int16 int32 int64 uint8 uint16 uint32 uint64
//    halffloat float double
//    day_time_interval month_interval
//    utf8 binary large_utf8 large_binary
// [2] Required Identity Type Constructable.
// Must/May be good to construct from its own type with specification.
//    date32 date64 time32 time64 timestamp duration
//    decimal fixed_size_binary
// [3] List-like type. Provide a data type and construct builder.
// [4] Struct type. Need to construct the builder for each fields.
// [5] Map. Need to construct the index and value builder.
// [6] DenseUnion, SparseUnion.
// [7] dictionary: Need to specify the exact typing, and establish builders.
//      DictionaryBuilder<T>
// [8] Extension: No support. Extension type does not have a builder.

template <typename DataType>
class BuilderFactory {
  BuilderFactory(){}
  BuilderFactory(const DataType & dataType):dataType(dataType){}

 private:
  std::shared_ptr<DataType> dataType;

};

//template <typename T>
//std::shared_ptr<arrow::ArrayBuilder>
//_CreateArrayBuilderDefaultConstructableFromField(
//    const std::shared_ptr<arrow::Field>& field,
//    arrow::MemoryPool* mem_pool = arrow::default_memory_pool()) {
//  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
//  auto datatype = field->type();
//  return std::make_shared<BuilderType>(datatype);
//}
//
//template <typename T>
//constexpr int CreateArrayBuilderFromFieldArbitrator() {
//  if constexpr (std::is_same_v<T, arrow::NullType> ||
//                arrow::is_number_type<T>::value ||
//                std::is_same_v<T, arrow::StringType>) {
//    return 1;
//  } else if constexpr (has_builder_type<T>::is_defalut_constructable_v) {
//    return 2;
//  }
//  return 0;
//}
//
//template <typename T, int switch_case_num>
//std::shared_ptr<arrow::ArrayBuilder> CreateArrayBuilderFromFieldInternal(
//    const std::shared_ptr<arrow::Field>& field, arrow::MemoryPool* mem_pool) {
//  if constexpr (switch_case_num == 1) {
//    return _CreateDefaultBuilder<T>(mem_pool);
//  } else if constexpr (switch_case_num == 2) {
//    return _CreateArrayBuilderDefaultConstructableFromField<T>(field, mem_pool);
//  }
//  //  using BuilderType = typename arrow::TypeTraits<T>::BuilderType;
//  //  auto datatype = field->type();
//  //  return std::make_shared<BuilderType>(datatype);
//}
//
//std::shared_ptr<arrow::ArrayBuilder> CreateArrayBuilderFromField(
//    const std::shared_ptr<arrow::Field>& field,
//    arrow::MemoryPool* mem_pool = arrow::default_memory_pool()) {
//  // Find the class of the field.
//
//  auto enum_type = field->type()->id();
//  std::shared_ptr<arrow::ArrayBuilder> result;
//
//#undef HUSTLE_ARROW_TYPE_CASE_STMT
//#define HUSTLE_ARROW_TYPE_CASE_STMT(arrow_type)                                \
//  {                                                                            \
//    constexpr int switch_case_num =                                            \
//        CreateArrayBuilderFromFieldArbitrator<arrow_type>();                   \
//    result = CreateArrayBuilderFromFieldInternal<arrow_type, switch_case_num>( \
//        field, mem_pool);                                                      \
//  }
//  HUSTLE_SWITCH_ARROW_TYPE(enum_type)
//#undef HUSTLE_ARROW_TYPE_CASE_STMT
//  return result;
//}


};  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H
