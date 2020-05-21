# Declare micro-libs:
add_library(hustle_src_scheduler_Node ../empty_src.cpp Node.hpp)
add_library(hustle_src_scheduler_Scheduler Scheduler.cpp Scheduler.hpp)
add_library(hustle_src_scheduler_SchedulerFlags SchedulerFlags.cpp SchedulerFlags.hpp)
add_library(hustle_src_scheduler_SchedulerInterface ../empty_src.cpp SchedulerInterface.hpp)
add_library(hustle_src_scheduler_SchedulerMessage ../empty_src.cpp SchedulerMessage.hpp)
add_library(hustle_src_scheduler_SchedulerTypedefs ../empty_src.cpp SchedulerTypedefs.hpp)
add_library(hustle_src_scheduler_Task ../empty_src.cpp Task.hpp)
add_library(hustle_src_scheduler_TaskDescription ../empty_src.cpp TaskDescription.hpp)
add_library(hustle_src_scheduler_TaskName ../empty_src.cpp TaskName.hpp)
add_library(hustle_src_scheduler_TaskStatistics TaskStatistics.cpp TaskStatistics.hpp)
add_library(hustle_src_scheduler_Worker Worker.cpp Worker.hpp)
add_library(hustle_src_scheduler_WorkerMessage ../empty_src.cpp WorkerMessage.hpp)

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

add_executable(hustle_scheduler_test "tests/SchedulerTest.cpp")
target_link_libraries(hustle_scheduler_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        hustle_src_scheduler_Scheduler
        hustle_src_operators
        )
add_test(HustleSchedulerTest hustle_scheduler_test)