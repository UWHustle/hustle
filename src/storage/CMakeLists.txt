add_library(
        hustle_src_storage
        table.h table.cc
        block.h block.cc
        util.h  util.cc
        cmemlog.h cmemlog.cc
        record_transformer.h
)

target_include_directories(hustle_src_storage PUBLIC ${ARROW_INCLUDE_DIR} )
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


add_executable(hustle_util_test "tests/util_test.cc")
target_link_libraries(hustle_util_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_storage
        )
add_test(HustleUtilTest hustle_util_test)

add_executable(hustle_memlog_test "tests/memlog_test.cc")
target_link_libraries(hustle_memlog_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_storage
        )
add_test(HustleMemlogTest hustle_memlog_test)
