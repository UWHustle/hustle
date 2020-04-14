add_library(
        resolver
        Plan.h Plan.cpp
        Resolver.h Resolver.cpp
)

target_link_libraries(resolver
        parser
)
#
#add_executable(hustle_resolver_simple_test "tests/HustleResolver_simple_test.cpp")
#target_link_libraries(hustle_resolver_simple_test
#        gtest
#        gtest_main
#        gmock
#        catalog
#        sqlite3
#        hustleDB
#        table
#        resolver
#        )
#add_test(HustleResolver_test hustle_resolver_simple_test)
#
#
#add_executable(hustle_resolver_ssb_test "tests/HustleResolver_ssb_test.cpp")
#target_link_libraries(hustle_resolver_ssb_test
#        gtest
#        gtest_main
#        gmock
#        catalog
#        sqlite3
#        hustleDB
#        table
#        resolver
#        )
#add_test(HustleResolver_test hustle_resolver_ssb_test)
