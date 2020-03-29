add_library(
        operators
        Operator.h
        Join.h Join.cpp
        Aggregate.h Aggregate.cpp
        Select.h Select.cpp
        OrderBy.cpp OrderBy.h
        Project.cpp Project.h
)
target_include_directories(operators PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(operators PUBLIC
        table
        ${ARROW_SHARED_LIB}
)


add_executable(hustle_operator_test "tests/HustleOperator_test.cpp")
target_link_libraries(hustle_operator_test
        gtest
        gtest_main
        gmock
        table
        operators
        )
add_test(HustleOperator_test hustle_operator_test)

add_executable(hustle_projection_test "tests/ProjectionTest.cpp")
target_link_libraries(hustle_projection_test
        gtest
        gtest_main
        gmock
        table
        operators
        )
add_test(HustleProjection_test hustle_projection_test)

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
