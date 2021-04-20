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

#include "scheduler/scheduler.h"

#include <filesystem>
#include <iostream>

#include "execution/execution_plan.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace testing;
using namespace hustle;

class HustleSchedulerTest : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(HustleSchedulerTest, test1) {
  //    EventProfiler<std::string> profiler;
  //
  //    auto *container = profiler.getContainer();
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
  //    profiler.summarizeToStream(std::cout);
}
