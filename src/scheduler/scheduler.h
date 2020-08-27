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

#ifndef PROJECT_SCHEDULER_SCHEDULER_HPP_
#define PROJECT_SCHEDULER_SCHEDULER_HPP_

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "scheduler/node.h"
#include "scheduler/scheduler_interface.h"
#include "scheduler/scheduler_message.h"
#include "scheduler/scheduler_typedefs.h"
#include "scheduler/task.h"
#include "scheduler/worker.h"
#include "scheduler/worker_message.h"
#include "utils/macros.h"
#include "utils/thread_safe_queue.h"

namespace hustle {

class Scheduler final : public SchedulerInterface {
 public:
  explicit Scheduler(const std::size_t num_workers,
                     const bool thread_pinning = false);

  static Scheduler &GlobalInstance();

  std::size_t getNumWorkers() const override;

  Continuation allocateContinuation() override;

  TaskID addTask(Task *task) override;

  TaskID addTask(Task *task, const NodeID dependency,
                 const NodeID dependent) override;

  TaskID addTaskWithDependent(Task *task, const NodeID dependent) override;

  TaskID addTaskWithDependency(Task *task, const NodeID dependency) override;

  void addLink(const NodeID dependency, const NodeID dependent) override;

  void fire(const Continuation entry) override;

  void sendMessage(SchedulerMessage *message) override;

  void start();

  void join();

  void execute();

  void clear();

  std::size_t getTotalNumTaskEvents() const;

  template <typename Functor>
  void forEachTaskEvent(const Functor &functor) const;

 private:
  inline TaskID allocateTaskID() {
    TaskID value = task_counter_.load(std::memory_order_relaxed);
    while (!task_counter_.compare_exchange_weak(value, value + 1,
                                                std::memory_order_relaxed)) {
    }
    return value;
  }

  inline Task *releaseTaskByID(const TaskID task_id);
  inline void addTaskIntoQueue(Task *task);
  inline void processTaskQueue();

  inline void addLinkUnsafe(const NodeID dependency, const NodeID dependent);
  inline void processDependentsUnsafe(
      const std::unordered_map<NodeID, Node>::iterator &node_it);
  inline void processDependents(const NodeID node_id);

  // Unsafe to use, but we might need it later.
  inline Continuation allocateContinuation(const NodeID dependency);

  std::unique_ptr<std::thread> thread_;

  const std::size_t num_workers_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::stack<WorkerID> available_workers_;

  std::deque<std::unique_ptr<Task>> task_queue_;

  std::unordered_map<TaskID, std::unique_ptr<Task>> tasks_;
  std::unordered_map<NodeID, Node> nodes_;

  ThreadSafeQueue<std::unique_ptr<SchedulerMessage>> scheduler_msg_queue_;

  std::atomic<TaskID> task_counter_;
  std::atomic<Continuation> continuation_counter_;
  std::atomic<bool> halt_;

  std::mutex tasks_mutex_;
  std::mutex nodes_mutex_;
  std::mutex queue_mutex_;

  DISALLOW_COPY_AND_ASSIGN(Scheduler);
};

// ----------------------------------------------------------------------------
// Implementations of template methods follow.

template <typename Functor>
void Scheduler::forEachTaskEvent(const Functor &functor) const {
  for (const auto &worker : workers_) {
    for (const auto &task_event : worker->getTaskEvents()) {
      functor(task_event);
    }
  }
}

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_SCHEDULER_HPP_
