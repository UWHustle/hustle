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

#ifndef PROJECT_SCHEDULER_NODE_HPP_
#define PROJECT_SCHEDULER_NODE_HPP_

#include <unordered_set>
#include <utility>

#include "glog/logging.h"
#include "scheduler/scheduler_typedefs.h"
#include "utils/container_util.h"
#include "utils/macros.h"

namespace hustle {

class Node {
 public:
  Node() : dependency_count_(0) {}

  inline void setChild(const NodeID child_id, Node& child) {
    DCHECK_EQ(0uL, child.dependency_count_);
    DCHECK_EQ(0uL, child.dependents_.size());

    child.dependents_ = std::move(dependents_);
    child.dependency_count_ = 1;
    dependents_ = {child_id};
  }

  inline bool hasDependent(const NodeID dependent) {
    return ContainsKey(dependents_, dependent);
  }

  inline void addDependent(const NodeID dependent) {
    dependents_.insert(dependent);
  }

  inline void incDependency() { ++dependency_count_; }

  inline void decDependency() {
    DCHECK_GT(dependency_count_, 0u);
    --dependency_count_;
  }

  inline std::size_t getDependencyCount() const { return dependency_count_; }

  inline const std::unordered_set<NodeID>& getDependents() const {
    return dependents_;
  }

 private:
  std::unordered_set<NodeID> dependents_;
  std::size_t dependency_count_;

  DISALLOW_COPY_AND_ASSIGN(Node);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_NODE_HPP_
