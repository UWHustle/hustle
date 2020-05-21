add_library(hustle_src_optimizer_ExecutionPlan
        ExecutionPlan.cpp ExecutionPlan.hpp)

target_include_directories(hustle_src_optimizer_ExecutionPlan PUBLIC
        ${ARROW_INCLUDE_DIR})

target_link_libraries(hustle_src_optimizer_ExecutionPlan PUBLIC
        ${GFLAGS_LIB_NAME}
        ${ARROW_SHARED_LIB}
        glog
        hustle_src_scheduler_SchedulerInterface
        hustle_src_scheduler_SchedulerTypedefs
        hustle_src_scheduler_Task
        hustle_src_utils_EventProfiler
        hustle_src_utils_Macros
        hustle_src_utils_SyncStream)