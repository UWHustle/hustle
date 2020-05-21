#include <iostream>
#include <iostream>
#include <filesystem>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

//#include "scheduler/SchedulerInterface.hpp"
#include "scheduler/Scheduler.hpp"
#include "execution/ExecutionPlan.hpp"

using namespace testing;
using namespace hustle;

class HustleSchedulerTest : public testing::Test {
protected:

  void SetUp() override {

  }

};

TEST_F(HustleSchedulerTest, test1) {

//    EventProfiler<std::string> simple_profiler;
//
//    auto *container = simple_profiler.getContainer();
//    container->startEvent("overall");

  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.start();

  int x = 0;

  for (int i = 0; i < 10; ++i) {
    auto task = [&x, i](Task *ctx) {
      for (int j = 0; j < 1000; j++) {
        x++;
      }
    };
    auto task_chain = CreateTaskChain(CreateLambdaTask(task));
    scheduler.addTask(task_chain);
  }

  scheduler.join();
  std::cout << x << std::endl;
//    container->endEvent("overall");
//    simple_profiler.summarizeToStream(std::cout);
}


