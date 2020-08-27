# Declare micro-libs:
add_library(hustle_src_scheduler_Node ../empty_src.cc node.h)
add_library(hustle_src_scheduler_Scheduler scheduler.cc scheduler.h)
add_library(hustle_src_scheduler_SchedulerFlags scheduler_flags.cc scheduler_flags.h)
add_library(hustle_src_scheduler_SchedulerInterface ../empty_src.cc scheduler_interface.h)
add_library(hustle_src_scheduler_SchedulerMessage ../empty_src.cc scheduler_message.h)
add_library(hustle_src_scheduler_SchedulerTypedefs ../empty_src.cc scheduler_typedefs.h)
add_library(hustle_src_scheduler_Task ../empty_src.cc task.h)
add_library(hustle_src_scheduler_TaskDescription ../empty_src.cc task_description.h)
add_library(hustle_src_scheduler_TaskName ../empty_src.cc task_name.h)
add_library(hustle_src_scheduler_TaskStatistics task_statistics.cc task_statistics.h)
add_library(hustle_src_scheduler_Worker worker.cc worker.h)
add_library(hustle_src_scheduler_WorkerMessage ../empty_src.cc worker_message.h)

# Link dependencies:
target_link_libraries(hustle_src_scheduler_Node
                      glog
                      hustle_src_scheduler_SchedulerTypedefs
                      hustle_src_utils_ContainerUtil
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_Scheduler
                      glog
                      hustle_src_scheduler_Node
                      hustle_src_scheduler_SchedulerFlags
                      hustle_src_scheduler_SchedulerInterface
                      hustle_src_scheduler_SchedulerMessage
                      hustle_src_scheduler_SchedulerTypedefs
                      hustle_src_scheduler_Task
                      hustle_src_scheduler_Worker
                      hustle_src_scheduler_WorkerMessage
                      hustle_src_utils_ContainerUtil
                      hustle_src_utils_Macros
                      hustle_src_utils_ThreadSafeQueue)
target_link_libraries(hustle_src_scheduler_SchedulerFlags
                      ${GFLAGS_LIB_NAME})
target_link_libraries(hustle_src_scheduler_SchedulerInterface
                      hustle_src_scheduler_SchedulerTypedefs
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_SchedulerMessage
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_Task
                      glog
                      hustle_src_scheduler_SchedulerInterface
                      hustle_src_scheduler_SchedulerTypedefs
                      hustle_src_scheduler_TaskDescription
                      hustle_src_utils_Macros
                      hustle_src_utils_meta_FunctionTraits)
target_link_libraries(hustle_src_scheduler_TaskDescription
                      glog
                      hustle_src_scheduler_TaskName
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_TaskName
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_TaskStatistics
                      hustle_src_scheduler_Scheduler
                      hustle_src_scheduler_TaskDescription
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_scheduler_Worker
                      ${GFLAGS_LIB_NAME}
                      glog
                      hustle_src_scheduler_SchedulerInterface
                      hustle_src_scheduler_SchedulerMessage
                      hustle_src_scheduler_Task
                      hustle_src_scheduler_WorkerMessage
                      hustle_src_threading_ThreadUtil
                      hustle_src_utils_Macros
                      hustle_src_utils_ThreadSafeQueue)
target_link_libraries(hustle_src_scheduler_WorkerMessage
                      hustle_src_utils_Macros)

add_executable(hustle_scheduler_test "tests/scheduler_test.cc")
target_link_libraries(hustle_scheduler_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_scheduler_Scheduler
        hustle_src_operators
        )
add_test(HustleSchedulerTest hustle_scheduler_test)