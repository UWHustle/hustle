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

#ifndef PROJECT_UTILITY_META_TYPE_LIST_HPP_
#define PROJECT_UTILITY_META_TYPE_LIST_HPP_

#include <cstddef>

#include "utils/meta/common.h"
#include "utils/meta/type_list_meta_functions.h"

namespace meta {

template <typename... Ts>
class TypeList;

namespace internal {

using EmptyList = TypeList<>;

template <typename... Ts>
class TypeListBase {
 public:
  // ---------------------------------------------------------------------------
  // Members

  static constexpr std::size_t length = sizeof...(Ts);

  using type = TypeList<Ts...>;
  using self = type;

  // ---------------------------------------------------------------------------
  // Meta methods

  template <template <typename...> class Host>
  using bind_to = Host<Ts...>;

  template <std::size_t... pos>
  using at = typename ElementAtImpl<
      self, TypeList<std::integral_constant<std::size_t, pos>...>>::type;

  template <std::size_t n>
  using take = typename TakeImpl<self, EmptyList, n>::type;

  template <std::size_t n>
  using skip = typename SkipImpl<self, n>::type;

  template <typename T>
  using push_front = TypeList<T, Ts...>;

  template <typename T>
  using push_back = TypeList<Ts..., T>;

  template <typename T>
  using contains = EqualsAny<T, Ts...>;

  template <typename... DumbT>
  using unique = typename UniqueImpl<EmptyList, self, DumbT...>::type;

  template <typename TL>
  using append = typename AppendImpl<self, TL>::type;

  template <typename TL>
  using cartesian_product = typename CartesianProductImpl<self, TL>::type;

  template <typename Subtrahend>
  using subtract = typename SubtractImpl<EmptyList, self, Subtrahend>::type;

  template <template <typename...> class Op>
  using map = TypeList<typename Op<Ts>::type...>;

  template <template <typename...> class Op>
  using flatmap = typename FlatmapImpl<EmptyList, self, Op>::type;

  template <template <typename...> class Op>
  using filter = typename FilterImpl<EmptyList, self, Op>::type;

  template <template <typename...> class Op>
  using filtermap = typename FiltermapImpl<EmptyList, self, Op>::type;

  template <typename... DumbT>
  using flatten = typename FlattenImpl<EmptyList, self, DumbT...>::type;

  template <typename... DumbT>
  using flatten_once =
      typename FlattenOnceImpl<EmptyList, self, DumbT...>::type;

  template <template <typename...> class Op, typename InitT>
  using foldl = typename FoldlImpl<InitT, self, Op>::type;

  template <typename TL>
  using zip = typename ZipImpl<EmptyList, self, TL>::type;

  template <typename TL, template <typename...> class Op>
  using zip_with = typename ZipWithImpl<EmptyList, self, TL, Op>::type;

  template <typename T>
  using as_sequence = typename AsSequenceImpl<T, Ts...>::type;

  // ---------------------------------------------------------------------------
  // Static methods

  template <typename Functor>
  static inline void ForEach(const Functor &functor) {
    ForEachImpl<length == 0>(functor);
  }

 private:
  template <bool empty, typename Functor>
  static inline void ForEachImpl(const Functor &functor,
                                 std::enable_if_t<empty> * = 0) {
    // No-op
  }

  template <bool empty, typename Functor>
  static inline void ForEachImpl(const Functor &functor,
                                 std::enable_if_t<!empty> * = 0) {
    functor(take<1>());
    skip<1>::ForEach(functor);
  }
};

}  // namespace internal

template <typename T, typename... Ts>
class TypeList<T, Ts...> : public internal::TypeListBase<T, Ts...> {
 public:
  using head = T;
  using tail = TypeList<Ts...>;
};

template <>
class TypeList<> : public internal::TypeListBase<> {};

}  // namespace meta

#endif  // PROJECT_UTILITY_META_TYPE_LIST_HPP_
