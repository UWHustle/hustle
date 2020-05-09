#ifndef PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_
#define PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_

#include <cstdint>
#include <cstring>
#include <string>

#include "../scheduler/TaskName.hpp"
#include "../utility/Macros.hpp"

#include "glog/logging.h"

namespace hustle {

enum class TaskType : std::uint16_t {
  kGeneral = 0,
  kPreprocessing,
  kRelationalOperator
};

class TaskDescription {
 public:
  TaskDescription() : task_name_(nullptr) {
    std::memset(&task_info_, 0, sizeof(TaskInfo));
  }

  TaskDescription(const bool profiling,
                  const bool cascade,
                  const TaskType task_type,
                  const std::uint32_t task_major_id,
                  const TaskName *task_name)
      : task_info_({ profiling,
                     cascade,
                     static_cast<std::uint16_t>(task_type),
                     task_major_id }),
        task_name_(task_name) {}

  inline bool enabledProfiling() const {
    return task_info_.profiling;
  }

  inline bool enabledCascadeProfiling() const {
    return task_info_.profiling && task_info_.cascade;
  }

  inline TaskType getTaskType() const {
    return static_cast<TaskType>(task_info_.task_type);
  }

  inline std::uint32_t getTaskMajorId() const {
    return task_info_.task_major_id;
  }

  inline const TaskName* getTaskName() const {
    return task_name_;
  }

  inline void setProfiling(const bool enabled) {
    task_info_.profiling = enabled;
  }

  inline void setCascade(const bool enabled) {
    task_info_.cascade = enabled;
  }

  inline void setTaskType(const TaskType task_type) {
    task_info_.task_type = static_cast<std::uint16_t>(task_type);
  }

  inline void setTaskMajorId(const std::uint32_t task_major_id) {
    task_info_.task_major_id = task_major_id;
  }

  inline void setTaskName(const TaskName *task_name) {
    task_name_ = task_name;
  }

  TaskDescription inherit() const {
    return TaskDescription(enabledProfiling(),
                           enabledCascadeProfiling(),
                           getTaskType(),
                           getTaskMajorId(),
                           getTaskName());
  }

 private:
  struct TaskInfo {
    std::uint64_t profiling : 1,
                  cascade : 1,
                  task_type : 30,
                  task_major_id : 32;
  };

  TaskInfo task_info_;
  const TaskName *task_name_;
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_
