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