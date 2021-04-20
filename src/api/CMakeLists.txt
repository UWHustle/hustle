add_library(
        hustleDB
        hustle_db.h hustle_db.cc
)

target_link_libraries(hustleDB PUBLIC
                        sqlite_utils
                        sqlite3
                        hustle_src_catalog  
                        hustle_src_scheduler_Scheduler)

add_executable(hustle_hustleDB_test "tests/hustle_db_test.cc")
target_link_libraries(hustle_hustleDB_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        hustleDB
        )
add_test(HustleDB_test hustle_hustleDB_test)

add_executable(hustle_SQL_test "tests/sql_test.cc")
target_link_libraries(hustle_SQL_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        hustleDB
        )
add_test(SQL_test hustle_SQL_test)

add_executable(hustle_SQL_MISC_test "tests/sql_test_misc.cc")
target_link_libraries(hustle_SQL_MISC_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        hustleDB
        )
add_test(SQL_MISC_test hustle_SQL_MISC_test)

add_executable(hustle_SQL_JOIN_test "tests/sql_join_test.cc")
target_link_libraries(hustle_SQL_JOIN_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        hustleDB
        )
add_test(SQL_JOIN_test hustle_SQL_JOIN_test)

