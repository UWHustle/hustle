## Import external projects

include(ExternalProject)
include(FetchContent)

## Download the SqLite3 amalgamation source, and build a lirbary

#ExternalProject_Add(sqlite3_src
#        URL https://www.sqlite.org/src/tarball/sqlite.tar.gz?r=release
#        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/sqlite3_src
#        CONFIGURE_COMMAND ../sqlite3_src/configure
#        BUILD_COMMAND make sqlite3.c
#        INSTALL_COMMAND ""
#)
##
#add_library(
#        sqlite33 SHARED IMPORTED
#        ${CMAKE_CURRENT_BINARY_DIR}/sqlite3_src/src/sqlite3_src-build/sqlite3.c
#        ${CMAKE_CURRENT_BINARY_DIR}/sqlite3_src/src/sqlite3_src-build/sqlite3.h
#)

## GoogleTest
FetchContent_Declare(
        googletest
        GIT_REPOSITORY  https://github.com/google/googletest.git
        GIT_TAG         release-1.10.0
)
FetchContent_MakeAvailable(googletest)

## ABSL
FetchContent_Declare(
        abseil
        GIT_REPOSITORY  https://github.com/abseil/abseil-cpp.git
        GIT_TAG         master
)
FetchContent_MakeAvailable(abseil)

## Hustle Apache Arrow
FetchContent_Declare(
        hustle_arrow
        GIT_REPOSITORY  https://github.com/UWQuickstep/arrow.git
)
FetchContent_MakeAvailable(hustle_arrow)

add_subdirectory(sqlite3)
