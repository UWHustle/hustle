# Declare micro-libs:
add_library(project_threading_Mutex ../empty_src.cpp Mutex.hpp)
add_library(project_threading_SpinMutex ../empty_src.cpp SpinMutex.hpp)
add_library(project_threading_SynchronizationLock ../empty_src.cpp SynchronizationLock.hpp)
add_library(project_threading_ThreadUtil ../empty_src.cpp ThreadUtil.hpp)

# Link dependencies:
target_link_libraries(project_threading_Mutex
                      project_utility_Macros)
target_link_libraries(project_threading_SpinMutex
                      project_threading_Mutex
                      project_utility_Macros)
target_link_libraries(project_threading_SynchronizationLock
                      project_utility_Macros)
target_link_libraries(project_threading_ThreadUtil
                      glog)
