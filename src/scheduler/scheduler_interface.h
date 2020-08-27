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

#ifndef PROJECT_SCHEDULER_SCHEDULER_INTERFACE_HPP_
#define PROJECT_SCHEDULER_SCHEDULER_INTERFACE_HPP_

#include <memory>

#include "scheduler/scheduler_typedefs.h"
#include "utils/macros.h"

namespace hustle {

class SchedulerMessage;
class Task;

class SchedulerInterface {
 public:
  SchedulerInterface() {}

  virtual std::size_t getNumWorkers() const = 0;

  virtual Continuation allocateContinuation() = 0;

  virtual TaskID addTask(Task *task) = 0;

  virtual TaskID addTask(Task *task, const NodeID dependency,
                         const NodeID dependent) = 0;

  virtual TaskID addTaskWithDependent(Task *task, const NodeID dependent) = 0;

  virtual TaskID addTaskWithDependency(Task *task, const NodeID dependency) = 0;

  virtual void addLink(const NodeID dependency, const NodeID dependent) = 0;

  virtual void sendMessage(SchedulerMessage *message) = 0;

  virtual void fire(const Continuation entry) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(SchedulerInterface);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_SCHEDULER_INTERFACE_HPP_
