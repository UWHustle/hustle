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

#include <iostream>
#include <map>
#include <vector>
#include "sqlite3/sqlite3.h"

namespace hustle {
namespace utils {

void init_sqlite3();

void open_sqlite3_db(const std::string &sqlitePath, sqlite3 **db, HustleMemLog *memlog);

void load_tables(sqlite3* db, std::vector<std::string> tables);

// Executes the sql query specified in sql on the database at sqlitePath,
// no output is returned.
bool execute_sqlite_query(sqlite3* db,
                          const std::string &sql);

// Executes the sql query specified in sql on the database at sqlitePath,
// the output is returned as a string.
std::string execute_sqlite_result(sqlite3* db,
                                  const std::string &sql);

void close_sqlite3(sqlite3* db);

void destroy_sqlite3();

}  // namespace utils
}  // namespace hustle
#endif  // HUSTLE_SQLITE_UTILS_H
