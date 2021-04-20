add_library(map_utils INTERFACE)

target_include_directories(
        map_utils INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(
        sqlite_utils
        sqlite_utils.h
        sqlite_utils.cc
)

add_library(
        sqlite_utils_parse
        sqlite_utils_parse.h
        sqlite_utils_parse.cc
)

target_link_libraries(
        sqlite_utils_parse
        absl::strings
        hustle_src_resolver
        hustle_src_storage
        sqlite3
)

target_link_libraries(
        sqlite_utils
        absl::strings
        hustle_src_resolver
        sqlite3
)


add_subdirectory(meta)
#add_subdirectory(arrow_hacks)

# Declare micro-libs:
add_library(hustle_src_utils_ContainerUtil ../empty_src.cc container_util.h)
add_library(hustle_src_utils_EventProfiler event_profiler.cc event_profiler.h)
add_library(hustle_src_utils_Macros ../empty_src.cc macros.h)
add_library(hustle_src_utils_SyncStream ../empty_src.cc sync_stream.h)
add_library(hustle_src_utils_ThreadSafeQueue ../empty_src.cc thread_safe_queue.h)
add_library(hustle_src_utils_ArrowComputeWrappers arrow_compute_wrappers.cc arrow_compute_wrappers.h)
#add_library(hustle_src_utils_ContextPool context_pool.cc context_pool.h)
add_library(hustle_src_utils_ParallelHashmap ../empty_src.cc parallel_hashmap/phmap.h)
add_library(hustle_src_utils_Config ../empty_src.cc config.h)
add_library(hustle_src_utils_skew ../empty_src.cc skew.h)
add_library(hustle_src_utils_bit_utils ../empty_src.cc bit_utils.h)
add_library(hustle_src_utils_string_utils ../empty_src.cc string_utils.h)
add_library(hustle_src_utils_parallel_utils ../empty_src.cc parallel_utils.h)
add_library(hustle_src_utils_disjoin_set_forest ../empty_src.cc disjoint_set_forest.h)

add_library(hustle_src_utils_BloomFilter ../empty_src.cc bloom_filter.h histogram.h)

find_path(LIB_CONFIG_DIR NAMES libconfig.h++)
find_library(LIB_CONFIG_LIB NAMES config++)

# Include Arrow
target_include_directories(hustle_src_utils_BloomFilter PUBLIC ${ARROW_INCLUDE_DIR})
target_include_directories(hustle_src_utils_ArrowComputeWrappers PUBLIC ${ARROW_INCLUDE_DIR})
target_include_directories(hustle_src_utils_Config PUBLIC ${LIB_CONFIG_DIR})
#target_include_directories(hustle_src_utils_ContextPool PUBLIC ${ARROW_INCLUDE_DIR})


# Link dependencies:
target_link_libraries(hustle_src_utils_EventProfiler
        glog
        hustle_src_threading_Mutex
        hustle_src_utils_Macros)
target_link_libraries(hustle_src_utils_SyncStream
        hustle_src_utils_Macros)
target_link_libraries(hustle_src_utils_ThreadSafeQueue
        hustle_src_utils_Macros)
target_link_libraries(hustle_src_utils_ArrowComputeWrappers
        absl::hash
        ${ARROW_SHARED_LIB})
target_link_libraries(hustle_src_utils_Config PUBLIC
        ${LIB_CONFIG_LIB}
        )
#target_link_libraries(hustle_src_utils_BloomFilter PUBLIC
#        ${ARROW_SHARED_LIB}
#        )

# Tests

#add_executable(hustle_src_utils_tests_BloomFilterTest "tests/bloom_filter_test.cc")
#target_link_libraries(hustle_src_utils_tests_BloomFilterTest
#        gtest
#        gtest_main
#        gmock
#        #        glog
#        ${ARROW_SHARED_LIB}
#        ${GFLAGS_LIB_NAME}
#        absl::hash
#        hustle_src_storage
#        hustle_src_utils_BloomFilter
#        )
#add_test(HustleBloomFilter_test hustle_src_utils_tests_BloomFilterTest)
