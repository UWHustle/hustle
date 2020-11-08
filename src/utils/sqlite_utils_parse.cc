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

#include <iostream>
#include "sqlite3/sqlite3.h"
#include "absl/strings/str_cat.h"
#include "resolver/cresolver.h"

namespace hustle {
namespace utils {

namespace {

// The callback function used by sqlite
static int callback_print_plan(void *result, int argc, char **argv,
                               char **azColName) {
  int i;
  for (i = 0; i < argc; i++) {
    if (std::strcmp(azColName[i], "detail") == 0) {
      absl::StrAppend((std::string *)result, argv[i]);
      absl::StrAppend((std::string *)result, "\n");
    }
  }
  return 0;
}

}  // namespace

Sqlite3Select* executeSqliteParse(const std::string &sqlitePath,
                                        const std::string &sql){
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  std::string result;

  rc = sqlite3_open(sqlitePath.c_str(), &db);

  if (rc) {
    fprintf(stderr, "Can't open sqlite catalog database: %s\n",
            sqlite3_errmsg(db));
    exit(-1);
  } else {
    //    fprintf(stdout, "Opened database successfully\n");
  }
  return sqlite3_select_parse(db, sql.c_str(), callback_print_plan, &result, &zErrMsg);
}

}  // namespace utils
}  // namespace hustle
