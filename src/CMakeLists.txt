add_executable(
        hustle
        hustle.cpp
)

target_link_libraries(hustle hustle_frontend)

set_target_properties(hustle
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(frontend)
