#include <iostream>

#include "sqlite3/sqlite3.h"

static int callback(void *data, int argc, char **argv, char **azColName) {
  fprintf(stdout, "%s \n", (const char *)data);
  return 0;
}

int main(int argc, char *argv[]) {
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  const char *data = "Callback function called";
  rc = sqlite3_open("test.db", &db);
  char *err_msg = 0;
  if (rc) {
    fprintf(stderr, "Can't open sqlite catalog database: %s\n",
            sqlite3_errmsg(db));
    exit(-1);
  } else {
    fprintf(stdout, "Opened database successfully\n");
  }
  char *sql =
      "CREATE TABLE Friends7(Id INTEGER PRIMARY KEY, Name TEXT, Address TEXT, "
      "Value INTEGER);"
      "INSERT INTO Friends7(Name, Address, Value) VALUES ('Tom', 'test', 1);"
      "INSERT INTO Friends7(Name, Address, Value) VALUES ('Rebecca', 'test', "
      "2);"
      "INSERT INTO Friends7(Name, Address, Value) VALUES ('Jim', 'test', 3);"
      "INSERT INTO Friends7(Name, Address, Value) VALUES ('Roger', 'test', 4);"
      "INSERT INTO Friends7(Name, Address, Value) VALUES ('Robert', 'test', "
      "5);";

  rc = sqlite3_exec(db, sql, 0, 0, NULL);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "table could exists already/query failed\n");

  } else {
    fprintf(stdout, "Table Friends created successfully\n");
  }

  fprintf(stderr, "Going to execute select\n");
  rc = sqlite3_exec(db, "SELECT SUM(F0.Value) FROM Friends7 AS F0 WHERE Id=3;",
                    callback, (void *)data, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
    exit(-1);
  } else {
    fprintf(stdout, "Query ececuted successfully\n");
  }
  sqlite3_close(db);
  return 0;
}
