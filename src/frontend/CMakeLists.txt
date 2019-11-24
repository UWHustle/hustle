add_library(
        hustle_frontend
        HustleFrontend.cpp HustleFrontend.h
)

target_link_libraries(
        hustle_frontend
        sqlite3
        absl::strings
)

add_executable(hustle_frontend_test "tests/HustleFrontend_test.cpp")
target_link_libraries(hustle_frontend_test
        gtest
        gtest_main
        hustle_frontend
        sqlite3
)
add_test(HustleFrontend_test hustle_frontend_test)
