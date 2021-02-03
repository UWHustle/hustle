add_library(
        hustle_src_operators
        join.h join.cc
        aggregate_const.h
        aggregate.h aggregate.cc
        hash_aggregate.h hash_aggregate.cc
        select.h select.cc
        join_graph.cc join_graph.h
        predicate.cc predicate.h
        operator.h
        expression.h
        operator_options.h
        aggregate_options.h
        fused/select_build_hash.cc fused/select_build_hash.h
        fused/filter_join.cc fused/filter_join.h
        utils/lazy_table.cc utils/lazy_table.h
        utils/operator_result.cc utils/operator_result.h
        utils/lip.cc utils/lip.h
        ../utils/bloom_filter.h
        ../utils/xxHash.h base_aggregate.h utils/aggregate_factory.cc utils/aggregate_factory.h)


target_include_directories(hustle_src_operators PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_operators PUBLIC
        hustle_src_storage
        hustle_src_utils_EventProfiler
        hustle_src_utils_ArrowComputeWrappers
        hustle_src_utils_Config
        hustle_src_utils_ParallelHashmap
        ${ARROW_SHARED_LIB}
        )



add_executable(hustle_join_test "tests/join_test.cc")
target_link_libraries(hustle_join_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleJoin_test hustle_join_test)

add_executable(hustle_aggregate_test "tests/aggregate_test.cc")
target_link_libraries(hustle_aggregate_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleAggregate_test hustle_aggregate_test)

add_executable(hustle_hash_aggregate_test "tests/hash_aggregate_test.cc")
target_link_libraries(hustle_hash_aggregate_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleHashAggregate_test hustle_hash_aggregate_test)

add_executable(hustle_select_test "tests/select_test.cc")
target_link_libraries(hustle_select_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleSelect_test hustle_select_test)

add_executable(hustle_select_build_hash_test "tests/select_build_hash_test.cc")
target_link_libraries(hustle_select_build_hash_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleSelectBuildHash_test hustle_select_build_hash_test)

add_executable(hustle_filter_join_test "tests/filter_join_test.cc")
target_link_libraries(hustle_filter_join_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_storage
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        )
add_test(HustleFilterJoin_test hustle_filter_join_test)

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
