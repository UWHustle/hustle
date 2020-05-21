add_library(
        hustle_src_parser
        Parser.h Parser.cpp
        ParseTree.h ParseTree.cpp
)

target_link_libraries(hustle_src_parser
        hustle_src_catalog
        )


add_executable(hustle_parser_simple_test tests/HustleParser_simple_test.cpp)
target_link_libraries(hustle_parser_simple_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_parser
        )
add_test(HustleParser_test hustle_parser_simple_test)

add_executable(hustle_parser_ssb_test "tests/HustleParser_ssb_test.cpp")
target_link_libraries(hustle_parser_ssb_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_parser
        )
add_test(HustleParser_test hustle_parser_ssb_test)
