add_library(
        hustle_src_catalog
        catalog.h catalog.cc
        column_schema.h
        table_schema.h table_schema.cc
)

target_link_libraries(
        hustle_src_catalog
        hustle_src_storage
        gtest
        absl::container
        absl::hash
        absl::strings
        absl::flat_hash_map
        map_utils
        sqlite3
        sqlite_utils
        ${ARROW_SHARED_LIB}
)

add_executable(hustle_catalog_test "tests/catalog_test.cc")
target_link_libraries(hustle_catalog_test
        gtest
        gtest_main
        gmock
        hustle_src_catalog
        hustle_src_storage
        sqlite3
        absl::container absl::hash
        absl::flat_hash_map
        hustleDB
        )
add_test(HustleCatalog_test hustle_catalog_test)
