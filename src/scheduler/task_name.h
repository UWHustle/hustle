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

#ifndef PROJECT_SCHEDULER_TASK_NAME_HPP_
#define PROJECT_SCHEDULER_TASK_NAME_HPP_

#include <string>

#include "utils/macros.h"

namespace hustle {

class TaskName {
 public:
  virtual ~TaskName() {}
  virtual std::string getName() const = 0;

 protected:
  TaskName() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(TaskName);
};

class SimpleTaskName : public TaskName {
 public:
  explicit SimpleTaskName(const std::string &name) : name_(name) {}

  std::string getName() const override { return name_; }

 private:
  const std::string name_;

  DISALLOW_COPY_AND_ASSIGN(SimpleTaskName);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_TASK_NAME_HPP_
