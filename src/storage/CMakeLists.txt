add_library(
        hustle_src_storage
        hustle_storable.h
        hustle_block.h
        base_block.h base_block.cc
        indexed_block.h indexed_block.cc
        hustle_index.h
        metadata_units/sma.h metadata_units/sma.cc
        hustle_table.h
        base_table.h base_table.cc
        index_aware_table.h index_aware_table.cc
        utils/util.h utils/util.cc
        cmemlog.h cmemlog.cc
        utils/record_utils.h hustle_storable.cc)

target_include_directories(hustle_src_storage PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_storage PUBLIC
        hustle_src_scheduler_Scheduler
        ${ARROW_SHARED_LIB}
        absl::strings
        absl::container
        absl::hash
        absl::flat_hash_map
        hustle_src_catalog
        hustleDB)


add_executable(hustle_table_test "tests/table_test.cc")
target_link_libraries(hustle_table_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        sqlite3
        hustle_src_storage
        )
add_test(HustleTableTest hustle_table_test)

add_executable(hustle_block_test "tests/block_test.cc")
target_link_libraries(hustle_block_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_storage
        )
add_test(HustleBlockTest hustle_block_test)

# TODO: Disabling this test will make Ubuntu 1604 compiles with g++ -std=c++2a.
#CMAKE_OSX_DEPLOYMENT_TARGET
if (${CMAKE_CXX_STANDARD} STREQUAL "20")
    message(STATUS "Disabled the hustle_util_test test to make sure target compiles.")
else ()
    add_executable(hustle_util_test "tests/util_test.cc")
    target_link_libraries(hustle_util_test
            gtest
            gtest_main
            gmock
            ${ARROW_SHARED_LIB}
            hustle_src_storage
            )
    add_test(HustleUtilTest hustle_util_test)
endif ()

add_executable(hustle_memlog_test "tests/memlog_test.cc")
target_link_libraries(hustle_memlog_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_storage
        )
add_test(HustleMemlogTest hustle_memlog_test)

add_executable(hustle_index_test "tests/index_test.cc")
target_link_libraries(hustle_index_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_storage
        )
add_test(HustleIndexTest hustle_index_test)
