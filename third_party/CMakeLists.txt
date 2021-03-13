## Import external projects

include(ExternalProject)
include(FetchContent)

## GoogleTest
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.10.0
)
FetchContent_MakeAvailable(googletest)

## ABSL
FetchContent_Declare(
        abseil
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG 20200923.3
)
FetchContent_MakeAvailable(abseil)

set(BENCHMARK_ENABLE_TESTING FALSE)
## Google Benchmark
FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG 73d4d5e8d6d449fc8663765a42aa8aeeee844489
)
FetchContent_MakeAvailable(googlebenchmark)


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
include_directories(nlohmann)
include_directories(better-enums)