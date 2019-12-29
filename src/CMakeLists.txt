add_executable(
        hustle_main
        main.cpp
)

target_link_libraries(hustle_main hustleDB)

set_target_properties(hustle_main
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(api)