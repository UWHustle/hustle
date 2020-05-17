# Declare micro-libs:
add_library(hustle_src_threading_Mutex ../empty_src.cpp Mutex.hpp)
add_library(hustle_src_threading_SpinMutex ../empty_src.cpp SpinMutex.hpp)
add_library(hustle_src_threading_SynchronizationLock ../empty_src.cpp SynchronizationLock.hpp)
add_library(hustle_src_threading_ThreadUtil ../empty_src.cpp ThreadUtil.hpp)

# Link dependencies:
target_link_libraries(hustle_src_threading_Mutex
                      hustle_src_utility_Macros)
target_link_libraries(hustle_src_threading_SpinMutex
                      hustle_src_threading_Mutex
                      hustle_src_utility_Macros)
target_link_libraries(hustle_src_threading_SynchronizationLock
                      hustle_src_utility_Macros)
target_link_libraries(hustle_src_threading_ThreadUtil
                      glog)
