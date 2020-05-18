add_library(map_utils INTERFACE)

target_include_directories(
        map_utils INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(
        sqlite_utils
        sqlite_utils.h
        sqlite_utils.cpp
)
target_link_libraries(
        sqlite_utils
        absl::strings
        sqlite3
)


add_subdirectory(meta)

# Declare micro-libs:
add_library(hustle_src_utility_ContainerUtil ../empty_src.cpp ContainerUtil.hpp)
add_library(hustle_src_utility_EventProfiler EventProfiler.cpp EventProfiler.hpp)
add_library(hustle_src_utility_Macros ../empty_src.cpp Macros.hpp)
add_library(hustle_src_utility_SyncStream ../empty_src.cpp SyncStream.hpp)
add_library(hustle_src_utility_ThreadSafeQueue ../empty_src.cpp ThreadSafeQueue.hpp)

# Link dependencies:
target_link_libraries(hustle_src_utility_EventProfiler
        glog
        hustle_src_threading_Mutex
        hustle_src_utility_Macros)
target_link_libraries(hustle_src_utility_SyncStream
        hustle_src_utility_Macros)
target_link_libraries(hustle_src_utility_ThreadSafeQueue
        hustle_src_utility_Macros)