## Import external projects

include(ExternalProject)
include(FetchContent)

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
#FetchContent_Declare(
#        hustle_arrow
#        GIT_REPOSITORY  https://github.com/UWQuickstep/arrow.git
#)
#FetchContent_MakeAvailable(hustle_arrow)

### NNG
#FetchContent_Declare(
#        nng
#        GIT_REPOSITORY  https://github.com/nanomsg/nng.git
#)
#FetchContent_MakeAvailable(nng)

add_subdirectory(sqlite3)
include_directories(sqlite3)

include_directories(cereal)