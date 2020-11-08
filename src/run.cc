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
  char *sql1, *sql2, *sql3;
  sql1 =
      "CREATE TABLE r0( c0 INTEGER PRIMARY KEY, c1 INTEGER, c2  INTEGER, "
      "c3 "
      "INTEGER );";
  sql2 =
      "CREATE TABLE r1( c0 INTEGER PRIMARY KEY, c1 INTEGER, c2  INTEGER, c3 "
      "INTEGER );";
  sql3 =
      "CREATE TABLE r2( c0 INTEGER PRIMARY KEY, c1 INTEGER, c2  INTEGER, c3 "
      "INTEGER );";

  printf("Going to create table: \n");
  rc = sqlite3_exec(db, sql1, callback, (void *)data, &zErrMsg);

  if (rc != SQLITE_OK) {
    sqlite3_free(zErrMsg);
  } else {
    fprintf(stdout, "Operation done successfully\n");
  }

  rc = sqlite3_exec(db, sql2, callback, (void *)data, &zErrMsg);

  if (rc != SQLITE_OK) {
    sqlite3_free(zErrMsg);
  } else {
    fprintf(stdout, "Operation done successfully\n");
  }

  rc = sqlite3_exec(db, sql3, callback, (void *)data, &zErrMsg);

  if (rc != SQLITE_OK) {
    sqlite3_free(zErrMsg);
  } else {
    fprintf(stdout, "Operation done successfully\n");
  }

  char *sql =
      "SELECT SUM(R0.c0) FROM r0 AS R0, r1 AS R1 WHERE R0.c1=R1.c1 and "
      "R0.c1=900 and "
      "R0.c2>9854376";
  rc = sqlite3_exec(db, sql, callback, (void *)data, &zErrMsg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  } else {
    fprintf(stdout, "Operation done successfully\n");
  }
  sqlite3_close(db);
  return 0;
}
