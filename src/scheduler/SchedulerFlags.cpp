#include "../scheduler/SchedulerFlags.hpp"

#include "gflags/gflags.h"

namespace hustle {

DEFINE_uint64(num_threads, 0, "Number of worker threads.");
DEFINE_uint64(initial_task_capacity, 0x10000, "Initial vertex capacity.");
DEFINE_uint64(initial_node_capacity, 0x10000, "Initial edge capacity.");

}  // namespace hustle
