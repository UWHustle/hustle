add_library(
        hustle_src_table
        table.h table.cpp
        block.h block.cpp
        util.h  util.cpp
)

target_include_directories(hustle_src_table PUBLIC ${ARROW_INCLUDE_DIR} )
target_link_libraries(hustle_src_table PUBLIC
        ${ARROW_SHARED_LIB}
        absl::strings
        absl::container
        absl::hash
        absl::flat_hash_map
        hustle_src_catalog)


add_executable(hustle_table_test "tests/TableTest.cpp")
target_link_libraries(hustle_table_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_table
        )
add_test(HustleTableTest hustle_table_test)

add_executable(hustle_block_test "tests/BlockTest.cpp")
target_link_libraries(hustle_block_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_table
        )
add_test(HustleBlockTest hustle_block_test)


add_executable(hustle_util_test "tests/UtilTest.cpp")
target_link_libraries(hustle_util_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_table
        )
add_test(HustleUtilTest hustle_util_test)
