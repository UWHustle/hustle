#include <iostream>

#include "sqlite3/sqlite3.h"
#include "absl/strings/str_cat.h"

namespace hustle {
namespace utils {

namespace {

// The callback function used by sqlite
static int
callback_print_plan(void *result, int argc, char **argv, char **azColName) {
  int i;
  for (i = 0; i < argc; i++) {
    if (std::strcmp(azColName[i], "detail") == 0) {
      absl::StrAppend((std::string *) result, argv[i]);
      absl::StrAppend((std::string *) result, "\n");
    }
  }
  return 0;
}

}

// Executes the sql statement on sqlite database at the sqlitePath path.
// Returns
std::string executeSqliteReturnOutputString(const std::string &sqlitePath,
                                            const std::string &sql) {

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

  rc = sqlite3_exec(db, sql.c_str(), callback_print_plan, &result, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
    exit(-1);
  } else {
//    fprintf(stdout, "Query ececuted successfully\n");
  }
  sqlite3_close(db);

  return result;
}

// Executes the sql statement on sqlite database at the sqlitePath path.
// No output is returned
bool
executeSqliteNoOutput(const std::string &sqlitePath, const std::string &sql) {

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;

  rc = sqlite3_open(sqlitePath.c_str(), &db);

  if (rc) {
    fprintf(stderr, "Can't open sqlite catalog database: %s\n",
            sqlite3_errmsg(db));
    return false;
  } else {
//    fprintf(stdout, "Opened database successfully\n");
  }

  rc = sqlite3_exec(db, sql.c_str(), nullptr, 0, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
    return false;
  } else {
//    fprintf(stdout, "Query ececuted successfully\n");
  }
  sqlite3_close(db);

  return true;
}


} // namespace utils
} // namespace hustle