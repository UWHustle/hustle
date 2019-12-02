/******************************************************************************
** This file is an amalgamation of many separate C source files from SQLite
** version 3.30.1.  By combining all the individual C code files into this
** single large file, the entire code can be compiled as a single translation
** unit.  This allows many compilers to do optimizations that would not be
** possible if the files were compiled separately.  Performance improvements
** of 5% or more are commonly seen when SQLite is compiled as a single
** translation unit.
**
** This file is all you need to compile SQLite.  To use SQLite in other
** programs, you need this file and the "sqlite3.h" header file that defines
** the programming interface to the SQLite library.  (If you do not have
** the "sqlite3.h" header file at hand, you will find a copy embedded within
** the text of this file.  Search for "Begin file sqlite3.h" to find the start
** of the embedded sqlite3.h header file.) Additional code files may be needed
** if you want a wrapper to interface SQLite with your choice of programming
** language. The code for the "sqlite3" command-line shell is also in a
** separate file. This file contains only code for the core SQLite library.
*/
#define SQLITE_CORE 1
#define SQLITE_AMALGAMATION 1
#ifndef SQLITE_PRIVATE
# define SQLITE_PRIVATE static
#endif
#include "sqlite3-1.c"
#include "sqlite3-2.c"
#include "sqlite3-3.c"
#include "sqlite3-4.c"
#include "sqlite3-5.c"
#include "sqlite3-6.c"
#include "sqlite3-7.c"
#if __LINE__!=225056
#undef SQLITE_SOURCE_ID
#define SQLITE_SOURCE_ID      "2019-10-10 20:19:45 18db032d058f1436ce3dea84081f4ee5a0f2259ad97301d43c426bc7f3dfalt2"
#endif
/* Return the source-id for this library */
SQLITE_API const char *sqlite3_sourceid(void){ return SQLITE_SOURCE_ID; }
/************************** End of sqlite3.c ******************************/
#include "sqlite3-8.c"
