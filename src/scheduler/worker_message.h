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

#ifndef PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_
#define PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_

#include <memory>

#include "utils/macros.h"

namespace hustle {

class Task;

enum class WorkerMessageType {
  kJoin = 0,
  kTask,
};

class WorkerMessage {
 public:
  inline WorkerMessageType getMessageType() const { return msg_type_; }

 protected:
  explicit WorkerMessage(const WorkerMessageType msg_type)
      : msg_type_(msg_type) {}

  const WorkerMessageType msg_type_;

 private:
  DISALLOW_COPY_AND_ASSIGN(WorkerMessage);
};

template <WorkerMessageType msg_type>
class SimpleWorkerMessage : public WorkerMessage {
 public:
  SimpleWorkerMessage() : WorkerMessage(msg_type) {}
};

using WorkerJoinMessage = SimpleWorkerMessage<WorkerMessageType::kJoin>;

class WorkerTaskMessage : public WorkerMessage {
 public:
  explicit WorkerTaskMessage(Task* task)
      : WorkerMessage(WorkerMessageType::kTask), task_(task) {}

  inline Task* getTask() const { return task_.get(); }

 private:
  std::unique_ptr<Task> task_;

  DISALLOW_COPY_AND_ASSIGN(WorkerTaskMessage);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_
