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
