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



add_executable(hustle_join_test "tests/JoinTest.cpp")
target_link_libraries(hustle_join_test
        gtest
        gtest_main
        gmock
        glog
        ${GFLAGS_LIB_NAME}
        table
        operators
        project_optimizer_ExecutionPlan
        project_scheduler_Scheduler
        )
add_test(HustleJoin_test hustle_join_test)

add_executable(hustle_aggregate_test "tests/AggregateTest.cpp")
target_link_libraries(hustle_aggregate_test
        gtest
        gtest_main
        gmock
        glog
        ${GFLAGS_LIB_NAME}
        table
        operators
        project_optimizer_ExecutionPlan
        project_scheduler_Scheduler
        )
add_test(HustleAggregate_test hustle_aggregate_test)

add_executable(hustle_select_test "tests/SelectTest.cpp")
target_link_libraries(hustle_select_test
        gtest
        gtest_main
        gmock
        glog
        ${GFLAGS_LIB_NAME}
        table
        operators
        project_optimizer_ExecutionPlan
        project_scheduler_Scheduler
        )
add_test(HustleSelect_test hustle_select_test)

add_executable(hustle_SSB_test "tests/SSBTest.cpp")
target_link_libraries(hustle_SSB_test
        gtest
        gtest_main
        gmock
        glog
        ${GFLAGS_LIB_NAME}
        table
        operators
        project_optimizer_ExecutionPlan
        project_scheduler_Scheduler
        )
add_test(HustleSSB_test hustle_SSB_test)

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
