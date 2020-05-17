#ifndef PROJECT_SCHEDULER_WORKER_HPP_
#define PROJECT_SCHEDULER_WORKER_HPP_

#include <chrono>
#include <memory>
#include <thread>
#include <tuple>
#include <vector>

#include "../scheduler/SchedulerInterface.hpp"
#include "../scheduler/SchedulerMessage.hpp"
#include "../scheduler/Task.hpp"
#include "../scheduler/WorkerMessage.hpp"
#include "../threading/ThreadUtil.hpp"
#include "../utility2/Macros.hpp"
#include "../utility2/ThreadSafeQueue.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace hustle {

DECLARE_uint64(task_event_capacity);

class Worker {
 public:
  using clock = std::chrono::steady_clock;
  using TaskEvent = std::tuple<TaskDescription, WorkerID,
                               clock::duration, clock::duration>;

  explicit Worker(const WorkerID worker_id,
                  const int cpu_id,
                  SchedulerInterface *scheduler)
      : worker_id_(worker_id),
        cpu_id_(cpu_id),
        scheduler_(scheduler) {
    task_events_.reserve(FLAGS_task_event_capacity);
  }

  inline WorkerID getWorkerID() const {
    return worker_id_;
  }

  void sendMessage(WorkerMessage *message) {
    DCHECK(message != nullptr);
    worker_msg_queue_.push(std::unique_ptr<WorkerMessage>(message));
  }

  void start() {
    DCHECK(thread_ == nullptr);
    thread_ = std::make_unique<std::thread>(
        [&]() -> void {
      this->execute();
    });
  }

  void join() {
    DCHECK(thread_ != nullptr);
    thread_->join();
    thread_.reset();
  }

  void execute() {
    if (cpu_id_ >= 0) {
      BindToCPU(cpu_id_);
    }

    bool quit = false;
    while (!quit) {
      std::unique_ptr<WorkerMessage> msg = worker_msg_queue_.popOne();
      DCHECK(msg != nullptr);

      switch (msg->getMessageType()) {
        case WorkerMessageType::kJoin: {
          quit = true;
          break;
        }
        case WorkerMessageType::kTask: {
          Task *task = static_cast<WorkerTaskMessage*>(msg.get())->getTask();
          DCHECK(task != nullptr);

          const clock::time_point start = clock::now();
          task->execute();

          if (task->enabledProfiling()) {
            task_events_.emplace_back(task->getDescription(),
                                      worker_id_,
                                      start - kZeroTime,
                                      clock::now() - kZeroTime);
          }

          scheduler_->sendMessage(
              new SchedulerTaskCompletionMessage(worker_id_, task->getID()));
          break;
        }
        default:
          LOG(FATAL) << "Unexpected WorkerMessage";
      }
    }
  }

  const std::vector<TaskEvent>& getTaskEvents() const {
    return task_events_;
  }

 private:
  const WorkerID worker_id_;
  const int cpu_id_;

  std::unique_ptr<std::thread> thread_;
  ThreadSafeQueue<std::unique_ptr<WorkerMessage>> worker_msg_queue_;

  std::vector<TaskEvent> task_events_;

  SchedulerInterface *scheduler_;

  static const clock::time_point kZeroTime;

  DISALLOW_COPY_AND_ASSIGN(Worker);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_WORKER_HPP_
