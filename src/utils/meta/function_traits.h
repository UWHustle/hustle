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

#ifndef PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_
#define PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_

#include <tuple>

namespace meta {

template <typename F>
struct FunctionTraits;

namespace internal {

template <typename T>
struct FunctionTraits_0;

template <typename ClassT, typename ReturnT, typename... Args>
struct FunctionTraits_0<ReturnT (ClassT::*)(Args...) const> {
  static constexpr std::size_t arity = sizeof...(Args);

  using return_type = ReturnT;

  template <std::size_t i>
  using argument_type =
      typename std::tuple_element<i, std::tuple<Args...>>::type;
};

}  // namespace internal

template <typename F>
struct FunctionTraits : internal::FunctionTraits_0<decltype(&F::operator())> {};

}  // namespace meta

#endif  // PROJECT_UTILITY_META_FUNCTION_TRAITS_HPP_
