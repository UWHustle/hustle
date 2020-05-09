#include "../scheduler/Scheduler.hpp"

#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <stack>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../scheduler/SchedulerFlags.hpp"
#include "../scheduler/SchedulerMessage.hpp"
#include "../scheduler/SchedulerTypedefs.hpp"
#include "../scheduler/Task.hpp"
#include "../scheduler/Worker.hpp"
#include "../utility/ContainerUtil.hpp"

namespace hustle {

Scheduler::Scheduler(const std::size_t num_workers,
                     const bool thread_pinning)
    : SchedulerInterface(),
      num_workers_(num_workers > 0 ? num_workers
                                   : std::thread::hardware_concurrency()) {
  std::vector<int> worker_cpu_affinities;
  if (thread_pinning) {
    const int num_cpu_cores = std::thread::hardware_concurrency();
    if (num_cpu_cores <= 0) {
      std::cerr << "Warning: Cannot detect the number of cpu cores. "
                << "Thread pinning operation will be a no-op.";
    } else if (num_cpu_cores < num_workers_) {
      std::cerr << "Warning: The number of cpu cores is less than the number "
                << "of workers. Thread pinning operation will be a no-op.";
    } else {
      for (std::size_t i = 0; i < num_workers_; ++i) {
        worker_cpu_affinities.emplace_back(i);
      }
    }
  }
  if (worker_cpu_affinities.empty()) {
    worker_cpu_affinities.assign(num_workers_, -1);
  }

  for (std::size_t i = 0; i < num_workers_; ++i) {
    workers_.emplace_back(
        std::make_unique<Worker>(i, worker_cpu_affinities[i], this));
  }

  tasks_.reserve(0x10000);
  nodes_.reserve(0x10000);
  clear();
}

Scheduler& Scheduler::GlobalInstance() {
  static Scheduler instance(FLAGS_num_threads, true);
  return instance;
}

void Scheduler::clear() {
  DCHECK(thread_ == nullptr);

  available_workers_ = std::stack<WorkerID>();
  for (std::size_t i = 0; i < num_workers_; ++i) {
    available_workers_.push(i);
  }

  task_queue_.clear();
  tasks_.clear();
  nodes_.clear();
  scheduler_msg_queue_.clear();

  task_counter_.store(1, std::memory_order_relaxed);
  continuation_counter_.store(kContinuationMask + 1, std::memory_order_relaxed);
  halt_.store(false, std::memory_order_relaxed);
}

std::size_t Scheduler::getNumWorkers() const {
  return num_workers_;
}

Continuation Scheduler::allocateContinuation() {
  Continuation value = continuation_counter_.load(std::memory_order_relaxed);
  while (!continuation_counter_.compare_exchange_weak(
              value, value + 1, std::memory_order_relaxed)) {}
  return value;
}

inline Continuation Scheduler::allocateContinuation(const NodeID dependency) {
  if (dependency == kInvalidNodeID) {
    return allocateContinuation();
  }

  const Continuation child_id = allocateContinuation();
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  DCHECK(ContainsKey(nodes_, dependency));
  nodes_.at(dependency).setChild(child_id, nodes_[child_id]);
  return child_id;
}

TaskID Scheduler::addTask(Task *task,
                          const NodeID dependency,
                          const NodeID dependent) {
  const TaskID task_id = allocateTaskID();
  task->setup(task_id, dependent, this);

  std::unique_lock<std::mutex> nodes_lock(nodes_mutex_);
  if (dependent != kInvalidNodeID) {
    nodes_[task_id].addDependent(dependent);
    nodes_[dependent].incDependency();
  }
  if (dependency == kInvalidNodeID) {
    // Immediate task.
    // TODO(jianqiao): Scheduling policy.
    nodes_lock.unlock();
    addTaskIntoQueue(task);
  } else {
    // Deferred task.
    addLinkUnsafe(dependency, task_id);
    nodes_lock.unlock();

    std::unique_lock<std::mutex> tasks_lock(tasks_mutex_);
    tasks_.emplace(task_id, std::unique_ptr<Task>(task));
    tasks_lock.unlock();
  }
  return task_id;
}

TaskID Scheduler::addTask(Task *task) {
  return addTask(task, kInvalidNodeID, kInvalidNodeID);
}

TaskID Scheduler::addTaskWithDependent(Task *task, const NodeID dependent) {
  return addTask(task, kInvalidNodeID, dependent);
}

TaskID Scheduler::addTaskWithDependency(Task *task, const NodeID dependency) {
  return addTask(task, dependency, kInvalidNodeID);
}

void Scheduler::addLink(const NodeID dependency, const NodeID dependent) {
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  addLinkUnsafe(dependency, dependent);
}

void Scheduler::fire(const Continuation entry) {
  processDependents(entry);
}

void Scheduler::sendMessage(SchedulerMessage *message) {
  DCHECK(message != nullptr);
  scheduler_msg_queue_.push(std::unique_ptr<SchedulerMessage>(message));
}

void Scheduler::start() {
  DCHECK(thread_ == nullptr);
  for (auto &worker : workers_) {
    worker->start();
  }
  thread_ = std::make_unique<std::thread>([&]{ this->execute(); });
}

void Scheduler::join() {
  DCHECK(thread_ != nullptr);
  scheduler_msg_queue_.push(std::make_unique<SchedulerJoinMessage>());
  thread_->join();
  thread_.reset();
  clear();
}

inline Task* Scheduler::releaseTaskByID(const TaskID task_id) {
  DCHECK(IsTaskID(task_id));
  std::lock_guard<std::mutex> tasks_lock(tasks_mutex_);
  const auto it = tasks_.find(task_id);
  DCHECK(it != tasks_.end());
  Task *task = it->second.release();
  tasks_.erase(it);
  return task;
}

inline void Scheduler::addTaskIntoQueue(Task *task) {
  {
    std::lock_guard<std::mutex> queue_lock(queue_mutex_);
    task_queue_.emplace_back(task);
  }
  scheduler_msg_queue_.push(std::make_unique<SchedulerNewTaskMessage>());
}

inline void Scheduler::processTaskQueue() {
  while (!available_workers_.empty()) {
    std::unique_ptr<Task> task;
    std::unique_lock<std::mutex> queue_lock(queue_mutex_);
    if (task_queue_.empty()) {
      queue_lock.unlock();
      return;
    } else {
      task = std::move(task_queue_.front());
      task_queue_.pop_front();
      queue_lock.unlock();
    }

    const std::size_t worker_id = available_workers_.top();
    available_workers_.pop();
    workers_[worker_id]->sendMessage(new WorkerTaskMessage(task.release()));
  }
}

inline void Scheduler::addLinkUnsafe(const NodeID dependency,
                                     const NodeID dependent) {
  auto &dependency_node = nodes_[dependency];
  if (!dependency_node.hasDependent(dependent)) {
    dependency_node.addDependent(dependent);
    nodes_[dependent].incDependency();
  }
}

inline void Scheduler::processDependentsUnsafe(
    const std::unordered_map<NodeID, Node>::iterator &node_it) {
  const auto &node = node_it->second;
  DCHECK_EQ(0u, node.getDependencyCount());

  for (const auto dependent : node.getDependents()) {
    DCHECK(ContainsKey(nodes_, dependent));
    auto dependent_node_it = nodes_.find(dependent);
    auto &dependent_node = dependent_node_it->second;
    dependent_node.decDependency();
    if (dependent_node.getDependencyCount() == 0) {
      if (IsTaskID(dependent)) {
        addTaskIntoQueue(releaseTaskByID(dependent));
      } else {
        processDependentsUnsafe(dependent_node_it);
      }
    }
  }
  nodes_.erase(node_it);
}

inline void Scheduler::processDependents(const NodeID node_id) {
  std::lock_guard<std::mutex> nodes_lock(nodes_mutex_);
  const auto node_it = nodes_.find(node_id);
  if (node_it != nodes_.end()) {
    processDependentsUnsafe(node_it);
  }
}

void Scheduler::execute() {
  while (true) {
    processTaskQueue();

    if (halt_.load(std::memory_order_relaxed) &&
        available_workers_.size() == num_workers_) {
      std::lock_guard<std::mutex> queue_lock(queue_mutex_);
      if (task_queue_.empty()) {
        break;
      }
    }

    std::unique_ptr<SchedulerMessage> msg = scheduler_msg_queue_.popOne();
    switch (msg->getMessageType()) {
      case SchedulerMessageType::kJoin: {
        halt_.store(true, std::memory_order_relaxed);
        break;
      }
      case SchedulerMessageType::kTaskCompletion: {
        SchedulerTaskCompletionMessage *task_completion =
            static_cast<SchedulerTaskCompletionMessage*>(msg.get());

        processDependents(task_completion->getTaskID());

        available_workers_.push(task_completion->getWorkerID());
        break;
      }
      case SchedulerMessageType::kNewTask: {
        break;
      }
      default:
        LOG(FATAL) << "Unexpected SchedulerMessage";
    }
  }

  // Stop workers.
  for (auto &worker : workers_) {
    worker->sendMessage(new WorkerJoinMessage());
  }
  for (auto &worker : workers_) {
    worker->join();
  }
}

std::size_t Scheduler::getTotalNumTaskEvents() const {
  std::size_t total = 0;
  for (const auto &worker : workers_) {
    total += worker->getTaskEvents().size();
  }
  return total;
}

}  // namespace hustle
