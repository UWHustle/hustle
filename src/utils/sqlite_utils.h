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

#ifndef HUSTLE_SQLITE_UTILS_H
#define HUSTLE_SQLITE_UTILS_H

namespace hustle {
namespace utils {

void initialize_sqlite3();

void loadTables(const std::string &sqlitePath);

// Executes the sql query specified in sql on the database at sqlitePath,
// no output is returned.
bool executeSqliteNoOutput(const std::string &sqlitePath,
                           const std::string &sql);

// Executes the sql query specified in sql on the database at sqlitePath,
// the output is returned as a string.
std::string executeSqliteReturnOutputString(const std::string &sqlitePath,
                                            const std::string &sql);

}  // namespace utils
}  // namespace hustle
#endif  // HUSTLE_SQLITE_UTILS_H
