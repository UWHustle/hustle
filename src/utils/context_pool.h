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

#ifndef HUSTLE_CONTEXT_H
#define HUSTLE_CONTEXT_H

#include <queue>

#include "utils/arrow_compute_wrappers.h"

namespace hustle {

class ContextPool {
 public:
  ContextPool();
  Context get_context();
  Context return_context(Context context);

 private:
  std::queue<Context> contexts_;
  std::mutex mutex_;
};

}  // namespace hustle

#endif  // HUSTLE_CONTEXT_H
