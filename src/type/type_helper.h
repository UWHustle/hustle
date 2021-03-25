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

#include "type_macros.h"

namespace hustle {

// TODO: Put requirement constraints that Functor must only have one argument
//  that accepts a (const T * ptr_) and will never use it.
//  This is the current constraint that lambda templates can't be initialized
//  with non-type parameter.

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

// Switcher: arrow enum types -> arrow DataType child classes.
// Must put the definition in header until g++ resolve the error.
// See: https://bit.ly/3bMbPG2
template <typename ArrowDataTypeSwitchFunctor>
void type_switcher(const std::shared_ptr<arrow::DataType> &dataType,
                   const ArrowDataTypeSwitchFunctor &func) {
#undef HUSTLE_ARROW_TYPE_CASE_STMT
#define HUSTLE_ARROW_TYPE_CASE_STMT(T) \
  { func((T *)nullptr); }
  HUSTLE_SWITCH_ARROW_TYPE(dataType->id());
#undef HUSTLE_ARROW_TYPE_CASE_STMT
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

// Methods to get arrow builder.
std::unique_ptr<arrow::ArrayBuilder> getBuilder(
    arrow::MemoryPool *memory_pool,
    const std::shared_ptr<arrow::DataType> &dataType);

std::unique_ptr<arrow::ArrayBuilder> getBuilder(
    const std::shared_ptr<arrow::DataType> &dataType);

};  // namespace hustle

#endif  // HUSTLE_TYPE_HELPER_H
