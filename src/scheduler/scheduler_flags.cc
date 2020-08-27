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

#include "scheduler/scheduler_flags.h"

#include "gflags/gflags.h"
namespace hustle {

DEFINE_uint64(num_threads, 0, "Number of worker threads.");
DEFINE_uint64(initial_task_capacity, 0x10000, "Initial vertex capacity.");
DEFINE_uint64(initial_node_capacity, 0x10000, "Initial edge capacity.");

}  // namespace hustle
