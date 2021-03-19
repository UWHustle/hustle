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

#ifndef HUSTLE_PARALLEL_UTILS_H
#define HUSTLE_PARALLEL_UTILS_H

namespace hustle {
namespace utils {

inline void ComputeBatchConfig(int &batch_size, int &num_batches,
                               int num_chunks, double num_parallel) {
  batch_size = num_chunks / num_parallel;
  if (batch_size == 0) batch_size = num_chunks;
  num_batches = num_chunks / batch_size + 1;
}

}  // namespace utils
}  // namespace hustle

#endif  // HUSTLE_PARALLEL_UTILS_H