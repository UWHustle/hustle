#ifndef PROJECT_SCHEDULER_NODE_HPP_
#define PROJECT_SCHEDULER_NODE_HPP_

#include <unordered_set>
#include <utility>

#include "../scheduler/SchedulerTypedefs.hpp"
#include "../utility2/ContainerUtil.hpp"
#include "../utility2/Macros.hpp"

#include "glog/logging.h"

namespace hustle {

class Node {
 public:
  Node() : dependency_count_(0) {}

  inline void setChild(const NodeID child_id, Node &child) {
    DCHECK_EQ(0uL, child.dependency_count_);
    DCHECK_EQ(0uL, child.dependents_.size());

    child.dependents_ = std::move(dependents_);
    child.dependency_count_ = 1;
    dependents_ = { child_id };
  }

  inline bool hasDependent(const NodeID dependent) {
    return ContainsKey(dependents_, dependent);
  }

  inline void addDependent(const NodeID dependent) {
    dependents_.insert(dependent);
  }

  inline void incDependency() {
    ++dependency_count_;
  }

  inline void decDependency() {
    DCHECK_GT(dependency_count_, 0u);
    --dependency_count_;
  }

  inline std::size_t getDependencyCount() const {
    return dependency_count_;
  }

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
