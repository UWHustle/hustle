add_library(
        table
        table.h table.cpp
        block.h block.cpp
        util.h  util.cpp
)

target_include_directories(table PUBLIC ${ARROW_INCLUDE_DIR} )
target_link_libraries(table PUBLIC
        ${ARROW_SHARED_LIB}
        absl::strings)

add_executable(hustle_table_test "tests/table_test.cpp")

add_executable(hustle_block_test "tests/block_test.cpp")

target_link_libraries(hustle_table_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        table
        )

target_link_libraries(hustle_block_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        table
        )

target_include_directories(hustle_table_test PUBLIC ${ARROW_INCLUDE_DIR})
add_test(HustleTableTest hustle_table_test)