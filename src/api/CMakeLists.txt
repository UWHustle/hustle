add_library(
        hustleDB
        HustleDB.h HustleDB.cpp
)

target_link_libraries(hustleDB sqlite_utils catalog)

#add_executable(hustle_hustleDB_test "tests/hustleDB_test.cpp")
#target_link_libraries(hustle_hustleDB_test
#        gtest
#        gtest_main
#        gmock
#        catalog
#        sqlite3
#        absl::container absl::hash
#        absl::flat_hash_map
#        hustleDB
#        )
#add_test(HustleDB_test hustle_hustleDB_test)