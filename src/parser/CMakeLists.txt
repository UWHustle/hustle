add_library(
        parser
        Parser.h
        ParseTree.h
)

target_link_libraries(parser
        catalog
)

add_executable(hustle_parser_simple_test "tests/HustleParser_simple_test.cpp")
target_link_libraries(hustle_parser_simple_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        hustleDB
        )
add_test(HustleParser_test hustle_parser_simple_test)

add_executable(hustle_parser_ssb_test "tests/HustleParser_ssb_test.cpp")
target_link_libraries(hustle_parser_ssb_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        hustleDB
        )
add_test(HustleParser_test hustle_parser_ssb_test)
