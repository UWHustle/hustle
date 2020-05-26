add_library(hustle_src_ssb
        workload.cpp
        q11.cpp)

add_executable(hustle_src_ssb_main workload.cpp)

target_include_directories(hustle_src_ssb PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_ssb PUBLIC
        hustle_src_table
        hustle_src_operators
        hustle_src_scheduler_Scheduler
        hustle_src_execution_ExecutionPlan
        hustle_src_utils_EventProfiler
        ${ARROW_SHARED_LIB}
)

add_definitions(-DHUSTLE_BUILD_DIRECTORY="\\"${CMAKE_SOURCE_DIR}\\"")
