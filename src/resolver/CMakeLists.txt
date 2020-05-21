add_library(
        hustle_src_resolver
        Plan.h Plan.cpp
        Resolver.h Resolver.cpp
)

target_link_libraries(hustle_src_resolver
        hustle_src_parser
)


add_executable(hustle_resolver_simple_test "tests/HustleResolver_simple_test.cpp")
target_link_libraries(hustle_resolver_simple_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_table
        hustle_src_resolver
        )
add_test(HustleResolver_test hustle_resolver_simple_test)


add_executable(hustle_resolver_ssb_test "tests/HustleResolver_ssb_test.cpp")
target_link_libraries(hustle_resolver_ssb_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_table
        hustle_src_resolver
        )
add_test(HustleResolver_test hustle_resolver_ssb_test)
