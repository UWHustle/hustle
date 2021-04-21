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

#ifndef HUSTLE_UTILS_ARROW_UTIL
#define HUSTLE_UTILS_ARROW_UTIL

#include <arrow/api.h>

#include <iostream>

/**
 * If a status outcome is an error, print its error message and throw an
 * expection. Otherwise, do nothing.
 *
 * The function_name and line_no parameters allow us to find exactly where
 * the error occured, since the same error can be thrown from different places.
 *
 * @param status Status outcome object
 * @param function_name name of the function that called evaluate_status
 * @param line_no Line number of the call to evaluate_status
 */
inline void evaluate_status(const arrow::Status& status,
                            const char* function_name, int line_no) {
  if (!status.ok()) {
    std::cout << "\nInvalid status: " << function_name << ", line " << line_no
              << std::endl;
    throw std::runtime_error(status.ToString());
  }
}



#endif  // HUSTLE_UTILS_ARROW_UTIL