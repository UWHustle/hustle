add_library(
        hustleDB
        HustleDB.h HustleDB.cpp
)

target_link_libraries(hustleDB sqlite_utils catalog)

add_executable(
        hustle_main
        main.cpp
)

target_link_libraries(hustle_main hustleDB)

set_target_properties(hustle_main
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_executable(HustleDB_test "tests/hustleDB_test.cpp")
target_link_libraries(HustleDB_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        cereal
        )
add_test(HustleDB_test HustleDB_test)


add_subdirectory(utils)
add_subdirectory(catalog)