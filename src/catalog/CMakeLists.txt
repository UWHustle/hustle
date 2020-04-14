add_library(
        catalog
        Catalog.h Catalog.cpp
        ColumnSchema.h
        TableSchema.h TableSchema.cpp
)

target_link_libraries(
        catalog
        gtest
        absl::container
        absl::hash
        absl::strings
        absl::flat_hash_map
        map_utils
        sqlite_utils
)

#add_executable(hustle_catalog_test "tests/HustleCatalog_test.cpp")
#target_link_libraries(hustle_catalog_test
#        gtest
#        gtest_main
#        gmock
#        catalog
#        sqlite3
#        absl::container absl::hash
#        absl::flat_hash_map
#        )
#add_test(HustleCatalog_test hustle_catalog_test)
