add_library(map_utils INTERFACE)

target_include_directories(
        map_utils INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(
        sqlite_utils
        sqlite_utils.h
        sqlite_utils.cpp
)
target_link_libraries(
        sqlite_utils
        absl::strings
        sqlite3
)


add_subdirectory(meta)
#add_subdirectory(arrow_hacks)

# Declare micro-libs:
add_library(hustle_src_utils_ContainerUtil ../empty_src.cpp ContainerUtil.hpp)
add_library(hustle_src_utils_EventProfiler EventProfiler.cpp EventProfiler.hpp)
add_library(hustle_src_utils_Macros ../empty_src.cpp Macros.hpp)
add_library(hustle_src_utils_SyncStream ../empty_src.cpp SyncStream.hpp)
add_library(hustle_src_utils_ThreadSafeQueue ../empty_src.cpp ThreadSafeQueue.hpp)
add_library(hustle_src_utils_ArrowComputeWrappers arrow_compute_wrappers.cpp arrow_compute_wrappers.h)
add_library(hustle_src_utils_ContextPool ContextPool.cpp ContextPool.h)
add_library(hustle_src_utils_ParallelHashmap ../empty_src.cpp parallel_hashmap/phmap.h)


#add_library(hustle_src_utils_BloomFilter BloomFilter.cpp BloomFilter.h)

# Include Arrow
#target_include_directories(hustle_src_utils_BloomFilter PUBLIC ${ARROW_INCLUDE_DIR})
target_include_directories(hustle_src_utils_ArrowComputeWrappers PUBLIC ${ARROW_INCLUDE_DIR})
target_include_directories(hustle_src_utils_ContextPool PUBLIC ${ARROW_INCLUDE_DIR})


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
#target_link_libraries(hustle_src_utils_BloomFilter PUBLIC
#        ${ARROW_SHARED_LIB}
#        )

# Tests

#add_executable(hustle_src_utils_tests_BloomFilterTest "tests/BloomFilterTest.cpp")
#target_link_libraries(hustle_src_utils_tests_BloomFilterTest
#        gtest
#        gtest_main
#        gmock
#        #        glog
#        ${ARROW_SHARED_LIB}
#        ${GFLAGS_LIB_NAME}
#        absl::hash
#        hustle_src_table
#        hustle_src_utils_BloomFilter
#        )
#add_test(HustleBloomFilter_test hustle_src_utils_tests_BloomFilterTest)