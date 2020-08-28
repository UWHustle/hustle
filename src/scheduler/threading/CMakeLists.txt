# Declare micro-libs:
add_library(hustle_src_threading_Mutex ../../empty_src.cc mutex.h)
add_library(hustle_src_threading_SpinMutex ../../empty_src.cc spin_mutex.h)
add_library(hustle_src_threading_SynchronizationLock ../../empty_src.cc synchronization_lock.h)
add_library(hustle_src_threading_ThreadUtil ../../empty_src.cc thread_util.h)

# Link dependencies:
target_link_libraries(hustle_src_threading_Mutex
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_threading_SpinMutex
                      hustle_src_threading_Mutex
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_threading_SynchronizationLock
                      hustle_src_utils_Macros)
target_link_libraries(hustle_src_threading_ThreadUtil
                      glog)
