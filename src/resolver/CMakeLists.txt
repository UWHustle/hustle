add_library(
        resolver
        Plan.h
        Resolver.h
)

target_link_libraries(resolver
        parser
)

#add_executable(hustle_parser_simple_test "tests/HustleParser_simple_test.cpp")
#target_link_libraries(hustle_parser_simple_test
#        gtest
#        gtest_main
#        gmock
#        catalog
#        sqlite3
#        hustleDB
#        )
#add_test(HustleParser_test hustle_parser_simple_test)
#
add_executable(hustle_resolver_ssb_test "tests/HustleResolver_ssb_test.cpp")
target_link_libraries(hustle_resolver_ssb_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        hustleDB
        )
add_test(HustleResolver_test hustle_resolver_ssb_test)
