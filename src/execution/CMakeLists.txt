
add_library(project_optimizer_ExecutionPlan ExecutionPlan.cpp ExecutionPlan.hpp)

target_link_libraries(project_optimizer_ExecutionPlan
                      ${GFLAGS_LIB_NAME}
                      glog
                      project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerTypedefs
                      project_scheduler_Task
                      project_utility_EventProfiler
                      project_utility_Macros
                      project_utility_SyncStream)