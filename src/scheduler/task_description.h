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

#ifndef PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_
#define PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_

#include <cstdint>
#include <cstring>
#include <string>

#include "glog/logging.h"
#include "scheduler/task_name.h"
#include "utils/macros.h"

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

  TaskDescription(const bool profiling, const bool cascade,
                  const TaskType task_type, const std::uint32_t task_major_id,
                  const TaskName *task_name)
      : task_info_({profiling, cascade, static_cast<std::uint16_t>(task_type),
                    task_major_id}),
        task_name_(task_name) {}

  inline bool enabledProfiling() const { return task_info_.profiling; }

  inline bool enabledCascadeProfiling() const {
    return task_info_.profiling && task_info_.cascade;
  }

  inline TaskType getTaskType() const {
    return static_cast<TaskType>(task_info_.task_type);
  }

  inline std::uint32_t getTaskMajorId() const {
    return task_info_.task_major_id;
  }

  inline const TaskName *getTaskName() const { return task_name_; }

  inline void setProfiling(const bool enabled) {
    task_info_.profiling = enabled;
  }

  inline void setCascade(const bool enabled) { task_info_.cascade = enabled; }

  inline void setTaskType(const TaskType task_type) {
    task_info_.task_type = static_cast<std::uint16_t>(task_type);
  }

  inline void setTaskMajorId(const std::uint32_t task_major_id) {
    task_info_.task_major_id = task_major_id;
  }

  inline void setTaskName(const TaskName *task_name) { task_name_ = task_name; }

  TaskDescription inherit() const {
    return TaskDescription(enabledProfiling(), enabledCascadeProfiling(),
                           getTaskType(), getTaskMajorId(), getTaskName());
  }

 private:
  struct TaskInfo {
    std::uint64_t profiling : 1, cascade : 1, task_type : 30,
        task_major_id : 32;
  };

  TaskInfo task_info_;
  const TaskName *task_name_;
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_TASK_DESCRIPTION_HPP_
