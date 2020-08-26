#ifndef PROJECT_SCHEDULER_TASK_NAME_HPP_
#define PROJECT_SCHEDULER_TASK_NAME_HPP_

#include <string>

#include "../utils/Macros.hpp"

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
