#ifndef PROJECT_SCHEDULER_TASK_HPP_
#define PROJECT_SCHEDULER_TASK_HPP_

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "../scheduler/SchedulerInterface.hpp"
#include "../scheduler/SchedulerTypedefs.hpp"
#include "../scheduler/TaskDescription.hpp"
#include "../utility/Macros.hpp"
#include "../utility/meta/FunctionTraits.hpp"

#include "glog/logging.h"

namespace hustle {

class Task {
 public:
  Task()
      : task_id_(kInvalidNodeID),
        continuation_(kInvalidNodeID),
        scheduler_(nullptr) {}

  virtual ~Task() {}

  virtual void execute() = 0;

  inline void spawnTask(Task *task) {
    task->inheritDescription(description_);
    getScheduler()->addTaskWithDependent(task, getContinuation());
  }

  template <typename Functor>
  inline void spawnLambdaTask(const Functor &functor);

  inline TaskID getID() const {
    DCHECK_NE(kInvalidNodeID, task_id_);
    return task_id_;
  }

  inline Continuation getContinuation() const {
    return continuation_;
  }

  inline SchedulerInterface* getScheduler() const {
    DCHECK(scheduler_ != nullptr);
    return scheduler_;
  }

  inline const TaskDescription& getDescription() const {
    return description_;
  }

  inline void inheritDescription(const TaskDescription &description) {
    description_ = description.inherit();
  }

  inline bool enabledProfiling() const {
    return description_.enabledProfiling();
  }

  inline void setProfiling(const bool enabled) {
    description_.setProfiling(enabled);
  }

  inline void setCascade(const bool enabled) {
    description_.setCascade(enabled);
  }

  inline void setTaskType(const TaskType task_type) {
    description_.setTaskType(task_type);
  }

  inline void setTaskMajorId(const std::uint32_t task_major_id) {
    description_.setTaskMajorId(task_major_id);
  }

  inline void setTaskName(const TaskName *task_name) {
    description_.setTaskName(task_name);
  }

 private:
  inline void setup(const TaskID task_id,
                    const Continuation continuation,
                    SchedulerInterface *scheduler) {
    DCHECK_EQ(kInvalidNodeID, task_id_);
    DCHECK_EQ(kInvalidNodeID, continuation_);
    DCHECK(scheduler_ == nullptr);
    task_id_ = task_id;
    continuation_ = continuation;
    scheduler_ = scheduler;
  }

  TaskID task_id_;
  Continuation continuation_;
  SchedulerInterface *scheduler_;
  TaskDescription description_;

  friend class Scheduler;
  friend class Worker;

  DISALLOW_COPY_AND_ASSIGN(Task);
};


template <typename Functor>
class LambdaTask : public Task {
 public:
  explicit LambdaTask(const Functor &functor)
      : functor_(functor) {
  }

  explicit LambdaTask(Functor &&functor)
      : functor_(std::move(functor)) {
  }

 protected:
  void execute() override {
    executeInternal<meta::FunctionTraits<Functor>>();
  }

 private:
  template <typename FT>
  void executeInternal(std::enable_if_t<FT::arity == 0>* = 0) {
    functor_();
  }

  template <typename FT>
  void executeInternal(std::enable_if_t<FT::arity == 1>* = 0) {
    functor_(this);
  }

  const Functor functor_;
};

template <typename Functor>
inline Task* CreateLambdaTask(const Functor &functor) {
  return new LambdaTask<Functor>(functor);
}

template <typename ...TaskTs>
inline Task* CreateTaskChain(TaskTs *...taskchain) {
  return CreateLambdaTask([tasks = std::vector<Task*>{taskchain...}](Task *ctx) {
    const std::size_t num_tasks = tasks.size();
    SchedulerInterface *scheduler = ctx->getScheduler();
    Continuation c_dependent = ctx->getContinuation();
    for (std::size_t i = 0; i < num_tasks; ++i) {
      const Continuation c_dependency =
          i == num_tasks-1 ? kInvalidNodeID : scheduler->allocateContinuation();

      Task *task = tasks[num_tasks-1-i];
      task->inheritDescription(ctx->getDescription());

      scheduler->addTask(task, c_dependency, c_dependent);
      c_dependent = c_dependency;
    }
  });
}

template <typename Functor>
inline void Task::spawnLambdaTask(const Functor &functor) {
  spawnTask(new LambdaTask<Functor>(functor));
}

}  // namespace hustle

#endif  // PROJECT_SCHEDULER_TASK_HPP_
