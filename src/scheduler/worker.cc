#include "scheduler/worker.h"

#include "gflags/gflags.h"

namespace hustle {

DEFINE_uint64(task_event_capacity, 0x10000, "Default task event list capacity");

const Worker::clock::time_point Worker::kZeroTime = Worker::clock::now();

}  // namespace hustle
