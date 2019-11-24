add_executable(
        hustle
        hustle.cpp
)

target_link_libraries(hustle hustle_frontend)

add_subdirectory(frontend)