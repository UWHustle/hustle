add_library(
        frontend
        ParseTree.h
)

target_link_libraries(frontend
        catalog
)

add_executable(hustle_frontend_test "tests/HustleFrontend_test.cpp")
target_link_libraries(hustle_frontend_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        hustleDB
        )
add_test(HustleFrontend_test hustle_frontend_test)

add_executable(hustle_frontend_ssb_test "tests/HustleFrontend_SSB_test.cpp")
target_link_libraries(hustle_frontend_ssb_test
        gtest
        gtest_main
        gmock
        catalog
        sqlite3
        hustleDB
        )
add_test(HustleFrontend_test hustle_frontend_ssb_test)
