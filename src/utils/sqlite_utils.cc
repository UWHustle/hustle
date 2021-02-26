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

#include "utils/sqlite_utils.h"

#include <iostream>

#include "absl/strings/str_cat.h"
#include "sqlite3/sqlite3.h"

namespace hustle {
namespace utils {

namespace {

// The callback function used by sqlite
static int callback_print_plan(void *result, int argc, char **argv,
                               char **azColName) {
  for (int i = 0; i < argc; i++) {
    if (std::strcmp(azColName[i], "detail") == 0) {
      absl::StrAppend((std::string *)result, argv[i]);
      absl::StrAppend((std::string *)result, "\n");
    }
  }
  return 0;
}

}  // namespace

void initialize_sqlite3() {
  int rc = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL config already in use and initialized.\n");
    return;
  }
  sqlite3_initialize();
}

void loadTables(const std::string &sqlitePath,
                std::vector<std::string> tables) {
  sqlite3 *db;
  int rc = sqlite3_open_v2(
      sqlitePath.c_str(), &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_CONFIG_MULTITHREAD,
      nullptr);
  for (auto const &table_name : tables) {
    sqlite3_load_hustle(db, table_name.c_str());
  }
  sqlite3_close(db);
}

// Executes the sql statement on sqlite database at the sqlitePath path.
// Returns
std::string executeSqliteReturnOutputString(const std::string &sqlitePath,
                                            const std::string &sql) {
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  std::string result;

  rc = sqlite3_open_v2(
      sqlitePath.c_str(), &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_CONFIG_MULTITHREAD,
      nullptr);

  if (rc) {
    fprintf(stderr, "Can't open sqlite catalog database: %s\n",
            sqlite3_errmsg(db));
  }

  rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", callback_print_plan,
                    &result, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }

  rc = sqlite3_exec(db, sql.c_str(), callback_print_plan, &result, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
  sqlite3_close(db);

  return result;
}

// Executes the sql statement on sqlite database at the sqlitePath path.
// No output is returned
bool executeSqliteNoOutput(const std::string &sqlitePath,
                           const std::string &sql) {
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  std::string result;

  rc = sqlite3_open_v2(
      sqlitePath.c_str(), &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
      nullptr);
  if (rc) {
    fprintf(stderr, "Can't open sqlite catalog database: %s\n",
            sqlite3_errmsg(db));
    return false;
  }
  sqlite3_busy_timeout(db, 2000);

  rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", callback_print_plan,
                    &result, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
  rc = sqlite3_exec(db, sql.c_str(), nullptr, 0, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
    return false;
  }
  sqlite3_close(db);
  return true;
}

}  // namespace utils
}  // namespace hustle