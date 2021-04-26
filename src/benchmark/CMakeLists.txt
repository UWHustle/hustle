find_package(benchmark REQUIRED)
add_library(hustle_src_benchmark
        ssb_workload.cc ssb_workload.h
        ssb_queries.cc ssb_queries.h
        tatp_workload.cc tatp_workload.h
        ssb_workload_lip.cc
        aggregate_workload.cc aggregate_workload.h ssb_queries.h ssb_queries.cc)

target_include_directories(hustle_src_benchmark PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_benchmark PUBLIC
        hustle_src_storage
        hustle_src_resolver
        hustle_src_scheduler_Scheduler
        hustle_src_optimizer_ExecutionPlan
        hustle_src_utils_EventProfiler
        hustle_src_utils_skew
        hustle_src_utils_Config
        sqlite_utils
        sqlite3
        ${ARROW_SHARED_LIB}
        )

add_executable(hustle_src_benchmark_main main.cc
        ../utils/bloom_filter.h
        ../utils/xxHash.h
        ../operators/join/hash_join.cc
        ../operators/join/hash_join.h
        aggregate_workload.cc aggregate_workload.h)
target_link_libraries(hustle_src_benchmark_main PUBLIC
        hustle_src_benchmark
        benchmark::benchmark
        ${GFLAGS_LIB_NAME}
        )

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
