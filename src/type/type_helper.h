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
//  #define HUSTLE_ARROW_TYPE_CASE_STMT(arrow_data_type_) \
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

template <typename T, typename... Ts>
using isOneOf = std::disjunction<std::is_same<T, Ts>...>;

template <typename DataType>
using is_string_type =
    isOneOf<DataType, arrow::StringType, arrow::LargeStringType>;

template <typename DataType>
using is_binary_type =
    isOneOf<DataType, arrow::BinaryType, arrow::LargeBinaryType>;

template <typename DataType, typename ReturnType>
using enable_if_has_c_type =
    std::enable_if_t<arrow::has_c_type<DataType>::value, ReturnType>;

template <typename DataType, typename ReturnType>
using enable_if_has_no_c_type =
    std::enable_if_t<!arrow::has_c_type<DataType>::value, ReturnType>;



// Create Array Builder
//    Use CreateBuilder as the central function.
//

namespace details {

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

std::shared_ptr<arrow::DataType> TestFields(arrow::Type::type type_enum);

}  // namespace details

std::shared_ptr<arrow::ArrayBuilder> getBuilder(
    const std::shared_ptr<arrow::DataType> &dataType);

template <typename DataTypeT>
class BuilderFactory {
 public:
  static constexpr details::BuildCategory CATEGORY =
      details::builder_category<DataTypeT>();
  static constexpr bool shouldThrowRunTimeError = false;

  BuilderFactory() {
    if constexpr (shouldThrowRunTimeError) {
      if constexpr (CATEGORY != 1) {
        throw std::runtime_error(
            std::string(
                "Builder factory can't use default constructor for type: ") +
            DataTypeT::type_name());
      }
    }
  };

  explicit BuilderFactory(std::shared_ptr<arrow::DataType> dataType)
      : _dataType(std::move(dataType)) {
    if constexpr (shouldThrowRunTimeError) {
      if constexpr (CATEGORY == 0 || CATEGORY == 7) {
        throw std::runtime_error(
            std::string("Builder factory can't support type: ") +
            DataTypeT::type_name());
      }
    }
  }

  explicit BuilderFactory(const std::shared_ptr<arrow::Field> &field)
      : _dataType(field->type()) {
    if constexpr (shouldThrowRunTimeError) {
      if constexpr (CATEGORY == 0 || CATEGORY == 7) {
        throw std::runtime_error(
            std::string("Builder factory can't support type: ") +
            DataTypeT::type_name());
      }
    }
  }

  arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> GetBuilder() {
    return GetBuilderInternal<CATEGORY>();
  }

 private:
  //
  // GetBuilder internal function template family.
  //
  // For each group, defines a template to handle the building process.
  // See details::BuildCategory for the type specification and classification.
  //
  using BuilderReturnType = arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>;

  template <details::BuildCategory category,
            details::BuildCategory categoryTarget>
  using RType = typename std::enable_if_t<(category == categoryTarget),
                                          BuilderReturnType>;

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::independent> GetBuilderInternal() {
    using BuilderType = typename arrow::TypeTraits<DataTypeT>::BuilderType;
    auto builder_ptr = std::make_shared<BuilderType>();
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::required_identity> GetBuilderInternal() {
    using BuilderType = typename arrow::TypeTraits<DataTypeT>::BuilderType;
    auto builder_ptr = std::make_shared<BuilderType>(
        this->_dataType, arrow::default_memory_pool());
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::list_like_type> GetBuilderInternal() {
    using BuilderType = typename arrow::TypeTraits<DataTypeT>::BuilderType;
    // TODO: List type should find the builder with this->_dataType 's nested
    // type.
    std::shared_ptr<arrow::ArrayBuilder> value_builder =
        std::make_shared<arrow::Int8Builder>();
    auto builder_ptr = std::make_shared<BuilderType>(
        arrow::default_memory_pool(), value_builder, this->_dataType);
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::struct_type> GetBuilderInternal() {
    std::shared_ptr<arrow::StructType> datatype =
        std::dynamic_pointer_cast<arrow::StructType>(this->_dataType);
    const int num_fields = datatype->num_fields();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    for (int i = 0; i < num_fields; i++) {
      auto sub_type = datatype->field(i)->type();
      auto sub_builder = getBuilder(sub_type);
      builders.emplace_back(sub_builder);
    }
    auto builder_ptr = std::make_shared<arrow::StructBuilder>(
        datatype, arrow::default_memory_pool(), builders);
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::map_type> GetBuilderInternal() {
    std::shared_ptr<arrow::MapType> data_type =
        std::dynamic_pointer_cast<arrow::MapType>(this->_dataType);

    auto key_type = data_type->key_type();
    auto item_type = data_type->item_type();

    auto key_builder = getBuilder(key_type);
    auto item_builer = getBuilder(item_type);

    auto builder_ptr = std::make_shared<arrow::MapBuilder>(
        arrow::default_memory_pool(), key_builder, item_builer);
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::union_type> GetBuilderInternal() {
    std::shared_ptr<arrow::UnionType> data_type =
        std::dynamic_pointer_cast<arrow::UnionType>(this->_dataType);
    const int num_fields = data_type->num_fields();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    for (int i = 0; i < num_fields; i++) {
      auto sub_type = data_type->field(i)->type();
      auto sub_builder = getBuilder(sub_type);
      builders.emplace_back(sub_builder);
    }
    auto builder_ptr = std::make_shared<arrow::SparseUnionBuilder>(
        arrow::default_memory_pool(), builders, data_type);
    arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
    return result;
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::dict_type> GetBuilderInternal() {
    std::shared_ptr<arrow::DictionaryType> data_type =
        std::dynamic_pointer_cast<arrow::DictionaryType>(this->_dataType);
    // Can only maps an integer to the corresponding dict type.
    // Now we just care about if the index.
    auto index_type = data_type->index_type();
    auto value_type = data_type->value_type();

    // TODO: (Future) Support all integer types.
    switch (index_type->id()) {
      case arrow::Type::INT8: {
        auto builder_ptr =
            std::make_shared<arrow::DictionaryBuilder<arrow::Int8Type>>(
                value_type, arrow::default_memory_pool());
        arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
        return result;
      }
      case arrow::Type::INT32: {
        auto builder_ptr =
            std::make_shared<arrow::DictionaryBuilder<arrow::Int32Type>>(
                value_type, arrow::default_memory_pool());
        arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> result(builder_ptr);
        return result;
      }
      default:
        return arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>(
            arrow::Status(arrow::StatusCode::NotImplemented,
                          "No support dict index other than int8 or int 32! "
                          "Got unexpected index type: " +
                              index_type->ToString()));
    }
  }

  template <details::BuildCategory cat>
  RType<cat, details::BuildCategory::extension_type> GetBuilderInternal() {
    // TODO: No support yet.
    return arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>(arrow::Status(
        arrow::StatusCode::NotImplemented, "No support for Extension type!"));
  }

  std::shared_ptr<arrow::DataType> _dataType;
};

};  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H
