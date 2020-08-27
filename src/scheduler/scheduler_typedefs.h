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

#ifndef PROJECT_SCHEDULER_SCHEDULER_TYPEDEFS_HPP_
#define PROJECT_SCHEDULER_SCHEDULER_TYPEDEFS_HPP_

#include <cstdint>

namespace hustle {

typedef std::size_t WorkerID;

typedef std::uint64_t NodeID;
typedef NodeID TaskID;
typedef NodeID Continuation;

constexpr Continuation kContinuationMask = 1ull << 63;

constexpr NodeID kInvalidNodeID = 0;

inline bool IsContinuation(const NodeID node_id) {
  return node_id & kContinuationMask;
}

inline bool IsTaskID(const NodeID node_id) { return !IsContinuation(node_id); }

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_SCHEDULER_TYPEDEFS_HPP_
