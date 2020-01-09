add_library(
        table
        table.h table.cpp
        block.h block.cpp
        util.h  util.cpp
        tests/table_test.h tests/table_test.cpp
)

target_include_directories(table PUBLIC ${ARROW_INCLUDE_DIR} )
target_link_libraries(table PUBLIC
        ${ARROW_SHARED_LIB}
        gtest)

#add_executable(hustle_table_test "tests/table_test.cpp")
#
#target_link_libraries(
#        hustle_table_test
#        gtest
#        gtest_main
#        gmock
#        table
#)

#add_test(table_test hustle_table_test)


