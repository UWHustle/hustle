add_library(
        hustle_frontend
        HustleFrontend.cpp HustleFrontend.h
)

target_link_libraries(
        hustle_frontend
        sqlite3
        absl::strings
)

target_include_directories(hustle_frontend PUBLIC ../../third_party/sqlite3)


add_executable(hustle_frontend_test "tests/HustleFrontend_test.cpp")
target_link_libraries(hustle_frontend_test
        gtest
        gtest_main
        hustle_frontend
        sqlite3
)
add_test(HustleFrontend_test hustle_frontend_test)
