#include "HustleFrontend.h"
#include "absl/strings/str_cat.h"

namespace hustle {
namespace frontend {

namespace {

// The callback function used by sqlite
static int callback(void *result, int argc, char **argv, char **azColName) {
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

std::string HustleFrontend::SqlToPlan(const std::string &sql) {
  sqlite3 *db;
  int rc;
  char *zErrMsg = 0;
  std::string result;

  rc = sqlite3_open(kDBFileName, &db);

  rc = sqlite3_exec(db, sql.c_str(), callback, &result, &zErrMsg);
  if (rc != SQLITE_OK) {
    std::cout << "SQL error: " << sqlite3_errmsg(db) << "\n";
    sqlite3_free(zErrMsg);
  }
  sqlite3_close(db);

  return result;
}

HustleFrontend::HustleFrontend() {

  sqlite3 *db;
  int rc;

  rc = sqlite3_open(kDBFileName, &db);

  if (rc) {
    std::cout << "Can't open database: " << sqlite3_errmsg(db) << "\n";
  } else {
    std::cout << "Open database successfully\n\n";
  }

  std::string statements[2] = {
      "create table R (A int, B int);",
      "create table S (A int, B int);"
  };

  char *zErrMsg = 0;
  for (const auto &statement : statements) {
    rc = sqlite3_exec(db, statement.c_str(), nullptr, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
      std::cout << "SQL error: " << sqlite3_errmsg(db) << "\n";
      sqlite3_free(zErrMsg);
      break;
    }
  }

  sqlite3_close(db);
}

} // namespace frontend
} // namespace hustle