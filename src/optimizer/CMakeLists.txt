add_library(
        hustle_src_optimizer
        reorder_joins.h reorder_joins.cc
)

target_link_libraries(hustle_src_optimizer PUBLIC
        hustle_src_operators
        hustle_src_storage
        hustle_src_catalog
        hustle_src_utils_disjoin_set_forest
        hustle_src_optimizer_ExecutionPlan
        )

add_executable(hustle_optimizer_test "tests/reorder_join_test.cc")
target_link_libraries(hustle_optimizer_test
        gtest
        gtest_main
        sqlite_utils_parse
        gmock
        hustle_src_catalog
        sqlite3
        hustleDB
        hustle_src_operators
        hustle_src_optimizer
        hustle_src_storage
        hustle_src_resolver
        )
add_test(HustleOptimizer_test hustle_optimizer_test)