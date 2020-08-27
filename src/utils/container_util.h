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

#ifndef PROJECT_UTILITY_CONTAINER_UTIL_HPP_
#define PROJECT_UTILITY_CONTAINER_UTIL_HPP_

#include <algorithm>
#include <atomic>
#include <memory>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

template <typename T>
using SharedVector = std::shared_ptr<std::vector<T>>;

template <typename T, typename... Args>
inline SharedVector<T> MakeSharedVector(Args &&... args) {
  return std::make_shared<std::vector<T>>(std::forward<Args>(args)...);
}

template <typename T>
using AtomicVector = std::vector<std::atomic<T>>;

template <typename Container, typename Key>
inline bool ContainsKey(const Container &container, const Key &key) {
  return container.find(key) != container.end();
}

template <typename Container, typename Key>
inline bool Contains(const Container &container, const Key &key) {
  const auto it = std::find(container.begin(), container.end(), key);
  return it != container.end();
}

template <typename T>
inline std::vector<T> Concatenate(const std::vector<T> &lhs,
                                  const std::vector<T> &rhs) {
  std::vector<T> results;
  results.reserve(lhs.size() + rhs.size());
  for (const T &value : lhs) {
    results.emplace_back(value);
  }
  for (const T &value : rhs) {
    results.emplace_back(value);
  }
  return results;
}

template <typename SourceContainer, typename TargetType>
inline void InsertAll(const SourceContainer &source,
                      std::vector<TargetType> *target) {
  for (const auto &value : source) {
    target->emplace_back(value);
  }
}

template <typename SourceContainer, typename TargetType, typename TargetEqual,
          typename TargetHash>
inline void InsertAll(
    const SourceContainer &source,
    std::unordered_set<TargetType, TargetEqual, TargetHash> *target) {
  for (const auto &value : source) {
    target->emplace(value);
  }
}

template <typename SourceContainer, typename TargetType, typename TargetEqual,
          typename TargetHash>
inline void EraseAll(
    std::unordered_set<TargetType, TargetEqual, TargetHash> *target,
    const SourceContainer &source) {
  for (const auto &value : source) {
    target->erase(value);
  }
}

#endif  // PROJECT_UTILITY_CONTAINER_UTIL_HPP_
