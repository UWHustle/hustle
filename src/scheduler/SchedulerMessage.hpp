#ifndef PROJECT_SCHEDULER_SCHEDULER_MESSAGE_HPP_
#define PROJECT_SCHEDULER_SCHEDULER_MESSAGE_HPP_

#include "../utility2/Macros.hpp"

namespace hustle {

class Task;

enum class SchedulerMessageType {
  kJoin = 0,
  kNewTask,
  kTaskCompletion
};

class SchedulerMessage {
 public:
  explicit SchedulerMessage(const SchedulerMessageType msg_type)
      : msg_type_(msg_type) {}

  inline SchedulerMessageType getMessageType() const {
    return msg_type_;
  }

 protected:
  const SchedulerMessageType msg_type_;

 private:
  DISALLOW_COPY_AND_ASSIGN(SchedulerMessage);
};

template <SchedulerMessageType msg_type>
class SimpleSchedulerMessage : public SchedulerMessage {
 public:
  SimpleSchedulerMessage()
      : SchedulerMessage(msg_type) {}
};

using SchedulerJoinMessage =
    SimpleSchedulerMessage<SchedulerMessageType::kJoin>;

using SchedulerNewTaskMessage =
    SimpleSchedulerMessage<SchedulerMessageType::kNewTask>;

class SchedulerTaskCompletionMessage : public SchedulerMessage {
 public:
  SchedulerTaskCompletionMessage(const WorkerID worker_id,
                                 const TaskID task_id)
      : SchedulerMessage(SchedulerMessageType::kTaskCompletion),
        worker_id_(worker_id),
        task_id_(task_id) {}

  inline WorkerID getWorkerID() const {
    return worker_id_;
  }

  inline TaskID getTaskID() const {
    return task_id_;
  }

 private:
  const WorkerID worker_id_;
  const TaskID task_id_;

  DISALLOW_COPY_AND_ASSIGN(SchedulerTaskCompletionMessage);
};

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_SCHEDULER_MESSAGE_HPP_
