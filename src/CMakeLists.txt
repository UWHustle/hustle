add_executable(
        hustle_main
        main.cpp
)

target_link_libraries(hustle_main hustleDB catalog table)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
find_package(Arrow REQUIRED)

set_target_properties(hustle_main
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(api)
add_subdirectory(frontend)
add_subdirectory(table)
add_subdirectory(operators)
