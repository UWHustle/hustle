add_library(
        hustle_src_resolver
        select_resolver.h select_resolver.cc
        cresolver.h cresolver.cc
)

target_link_libraries(hustle_src_resolver PUBLIC
        hustle_src_operators
        hustle_src_catalog
        hustle_src_optimizer_ExecutionPlan
        hustle_src_storage
        hustle_src_utils_EventProfiler
        hustleDB
)

add_executable(hustle_resolver_test "tests/resolver_test.cc")
target_link_libraries(hustle_resolver_test
        gtest
        gtest_main
        sqlite_utils_parse
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_operators
        hustle_src_storage
        hustle_src_resolver
        )
add_test(HustleSQLiteResolver_test hustle_resolver_test)

