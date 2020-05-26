add_library(
        hustle_src_bitweaving
        bitvector.cpp bitvector.h
        bitvector_block.cpp bitvector_block.h
        bitvector_iterator.cpp bitvector_iterator.h
        bwh_column_block.cpp bwh_column_block.h
        bwh_column_iterator.cpp bwh_column_iterator.h
        bwv_column_block.cpp bwv_column_block.h
        bwv_column_iterator.cpp bwv_column_iterator.h
        column.cpp column.h
        column_iterator.cpp column_iterator.h
        env.cpp env.h
        file.cpp file.h
        iterator.cpp iterator.h
        naive_column_block.cpp naive_column_block.h
        options.cpp options.h
        table.cpp table.h
        utility.cpp utility.h
        bitweaving_compare.cpp bitweaving_compare.h
        bitwise.h
        rdtsc.h
        doxy.h
        status.h
        types.h)


target_include_directories(hustle_src_bitweaving PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_bitweaving PUBLIC
        hustle_src_table
        hustle_src_operators
        ${ARROW_SHARED_LIB}
        )

set(TEST_SRC
    tests/bitweaving_test.cc
    tests/bitweaving_test_base.cpp tests/bitweaving_test_base.h
    tests/bitweaving_test_util.cc tests/bitweaving_test_util.h
    )

add_executable(hustle_bitweaving_test ${TEST_SRC})
target_link_libraries(hustle_bitweaving_test
        gtest
        gtest_main
        gmock
        #        glog
        ${GFLAGS_LIB_NAME}
        hustle_src_table
        hustle_src_operators
        hustle_src_optimizer_ExecutionPlan
        hustle_src_scheduler_Scheduler
        hustle_src_bitweaving
        )