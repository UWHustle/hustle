# Declare micro-libs:
add_library(project_scheduler_Node ../empty_src.cpp Node.hpp)
add_library(project_scheduler_Scheduler Scheduler.cpp Scheduler.hpp)
add_library(project_scheduler_SchedulerFlags SchedulerFlags.cpp SchedulerFlags.hpp)
add_library(project_scheduler_SchedulerInterface ../empty_src.cpp SchedulerInterface.hpp)
add_library(project_scheduler_SchedulerMessage ../empty_src.cpp SchedulerMessage.hpp)
add_library(project_scheduler_SchedulerTypedefs ../empty_src.cpp SchedulerTypedefs.hpp)
add_library(project_scheduler_Task ../empty_src.cpp Task.hpp)
add_library(project_scheduler_TaskDescription ../empty_src.cpp TaskDescription.hpp)
add_library(project_scheduler_TaskName ../empty_src.cpp TaskName.hpp)
add_library(project_scheduler_TaskStatistics TaskStatistics.cpp TaskStatistics.hpp)
add_library(project_scheduler_Worker Worker.cpp Worker.hpp)
add_library(project_scheduler_WorkerMessage ../empty_src.cpp WorkerMessage.hpp)

# Link dependencies:
target_link_libraries(project_scheduler_Node
                      glog
                      project_scheduler_SchedulerTypedefs
                      project_utility_ContainerUtil
                      project_utility_Macros)
target_link_libraries(project_scheduler_Scheduler
                      glog
                      project_scheduler_Node
                      project_scheduler_SchedulerFlags
                      project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerMessage
                      project_scheduler_SchedulerTypedefs
                      project_scheduler_Task
                      project_scheduler_Worker
                      project_scheduler_WorkerMessage
                      project_utility_ContainerUtil
                      project_utility_Macros
                      project_utility_ThreadSafeQueue)
target_link_libraries(project_scheduler_SchedulerFlags
                      ${GFLAGS_LIB_NAME})
target_link_libraries(project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerTypedefs
                      project_utility_Macros)
target_link_libraries(project_scheduler_SchedulerMessage
                      project_utility_Macros)
target_link_libraries(project_scheduler_Task
                      glog
                      project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerTypedefs
                      project_scheduler_TaskDescription
                      project_utility_Macros
                      project_utility_meta_FunctionTraits)
target_link_libraries(project_scheduler_TaskDescription
                      glog
                      project_scheduler_TaskName
                      project_utility_Macros)
target_link_libraries(project_scheduler_TaskName
                      project_utility_Macros)
target_link_libraries(project_scheduler_TaskStatistics
                      project_scheduler_Scheduler
                      project_scheduler_TaskDescription
                      project_utility_Macros)
target_link_libraries(project_scheduler_Worker
                      ${GFLAGS_LIB_NAME}
                      glog
                      project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerMessage
                      project_scheduler_Task
                      project_scheduler_WorkerMessage
                      project_threading_ThreadUtil
                      project_utility_Macros
                      project_utility_ThreadSafeQueue)
target_link_libraries(project_scheduler_WorkerMessage
                      project_utility_Macros)

add_executable(hustle_scheduler_test "tests/SchedulerTest.cpp")
target_link_libraries(hustle_scheduler_test
        gtest
        gtest_main
        gmock
        ${ARROW_SHARED_LIB}
        project_scheduler_Scheduler
        operators
        )
add_test(HustleSchedulerTest hustle_scheduler_test)