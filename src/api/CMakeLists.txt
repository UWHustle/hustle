add_library(
        hustleDB
        hustle_db.h hustle_db.cc
)

target_link_libraries(hustleDB PUBLIC
                        sqlite_utils 
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

