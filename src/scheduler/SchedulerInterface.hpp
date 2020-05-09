#ifndef PROJECT_SCHEDULER_SCHEDULER_INTERFACE_HPP_
#define PROJECT_SCHEDULER_SCHEDULER_INTERFACE_HPP_

#include <memory>

#include "../scheduler/SchedulerTypedefs.hpp"
#include "../utility/Macros.hpp"

namespace hustle {

class SchedulerMessage;
class Task;

class SchedulerInterface {
 public:
  SchedulerInterface() {}

  virtual std::size_t getNumWorkers() const = 0;

  virtual Continuation allocateContinuation() = 0;

  virtual TaskID addTask(Task *task) = 0;

  virtual TaskID addTask(Task *task, const NodeID dependency, const NodeID dependent) = 0;

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
