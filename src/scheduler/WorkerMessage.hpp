#ifndef PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_
#define PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_

#include <memory>

#include "../utils/Macros.hpp"

namespace hustle {

class Task;

enum class WorkerMessageType {
  kJoin = 0,
  kTask,
};

class WorkerMessage {
 public:
  inline WorkerMessageType getMessageType() const {
    return msg_type_;
  }

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
  SimpleWorkerMessage()
      : WorkerMessage(msg_type) {}
};

using WorkerJoinMessage = SimpleWorkerMessage<WorkerMessageType::kJoin>;

class WorkerTaskMessage : public WorkerMessage {
 public:
  explicit WorkerTaskMessage(Task *task)
      : WorkerMessage(WorkerMessageType::kTask),
        task_(task) {}

  inline Task* getTask() const {
    return task_.get();
  }

 private:
  std::unique_ptr<Task> task_;

  DISALLOW_COPY_AND_ASSIGN(WorkerTaskMessage);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_WORKER_MESSAGE_HPP_
