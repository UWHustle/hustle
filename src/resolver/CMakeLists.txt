add_library(
        hustle_src_resolver
        plan.h plan.cc
        resolver.h resolver.cc
        select_resolver.h select_resolver.cc
        cresolver.h cresolver.cc
)

target_link_libraries(hustle_src_resolver
        hustle_src_parser
        hustle_src_operators
        hustle_src_catalog
)


add_executable(hustle_resolver_simple_test "tests/resolver_simple_test.cc")
target_link_libraries(hustle_resolver_simple_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_storage
        hustle_src_resolver
        )
add_test(HustleResolver_test hustle_resolver_simple_test)

add_executable(hustle_resolver_test "tests/resolver_test.cc")
target_link_libraries(hustle_resolver_test
        gtest
        gtest_main
        sqlite_utils_parse
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_storage
        hustle_src_resolver
        )
add_test(HustleResolver_test hustle_resolver_test)


add_executable(hustle_resolver_ssb_test "tests/resolver_ssb_test.cc")
target_link_libraries(hustle_resolver_ssb_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_storage
        hustle_src_resolver
        )
add_test(HustleResolver_test hustle_resolver_ssb_test)
