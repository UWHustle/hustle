add_library(
        operators
        OperatorResult.cpp OperatorResult.h
        Join.h Join.cpp
        Aggregate.h Aggregate.cpp
        Select.h Select.cpp
        JoinGraph.cpp JoinGraph.h
        Predicate.cpp Predicate.h
        LazyTable.cpp LazyTable.h
        Operator.h)


target_include_directories(operators PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(operators PUBLIC
        table
        ${ARROW_SHARED_LIB}
)


add_library(hustle_operators_Operator Operator.h)
target_link_libraries(hustle_operators_Operator
        ${ARROW_SHARED_LIB}
        )

add_executable(hustle_join_test "tests/JoinTest.cpp")
target_link_libraries(hustle_join_test
        gtest
        gtest_main
        gmock
        table
        operators
        )
add_test(HustleJoin_test hustle_join_test)

add_executable(hustle_aggregate_test "tests/AggregateTest.cpp")
target_link_libraries(hustle_aggregate_test
        gtest
        gtest_main
        gmock
        table
        operators
        )
add_test(HustleAggregate_test hustle_aggregate_test)

add_executable(hustle_select_test "tests/SelectTest.cpp")
target_link_libraries(hustle_select_test
        gtest
        gtest_main
        gmock
        table
        operators
        )
add_test(HustleSelect_test hustle_select_test)

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
